package replication

import (
	"bufio"
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type transportClient struct{ *http.Client }

func newTransportClient() transportClient {
	return transportClient{&http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: time.Second}).DialContext, MaxIdleConnsPerHost: 4, IdleConnTimeout: time.Minute}, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}}
}

type packet struct {
	data     []byte
	snapshot bool
	control  bool
	file     *os.File
}

type peerSender struct {
	queue  chan packet
	ctx    context.Context
	cancel context.CancelFunc
}

// Call with peerMu held so no producer can enqueue after cancellation/drain.
func (n *Node) stopPeerSenders(id uint64) {
	for _, sender := range []peerSender{n.senders[id], n.controlSenders[id]} {
		if sender.cancel != nil {
			sender.cancel()
		}
	}
}

func (n *Node) send(messages []raftpb.Message) {
	for _, message := range messages {
		if message.To == 0 || message.To == n.cfg.ID {
			continue
		}
		data, err := message.Marshal()
		if err != nil {
			n.fail(err)
			return
		}
		var snapshotFile *os.File
		if message.Type == raftpb.MsgSnap {
			envelope, err := decodeSnapshot(message.Snapshot.Data)
			if err != nil {
				n.fail(err)
				return
			}
			if envelope.File != nil {
				snapshotFile, err = os.Open(snapshotPath(n.cfg.Dir, message.Snapshot.Metadata.Index, *envelope.File))
				if os.IsNotExist(err) {
					n.metrics.Event("snapshot_send_missing")
					n.raft.ReportSnapshot(message.To, raft.SnapshotFailure)
					continue
				}
				if err != nil {
					n.fail(err)
					return
				}
			}
		}
		// Raft tolerates message reordering. Keep quorum traffic independent of
		// append/snapshot HTTP requests, which can take longer than an election.
		control := message.Type != raftpb.MsgApp && message.Type != raftpb.MsgSnap
		n.peerMu.Lock()
		senders := n.senders
		if control {
			senders = n.controlSenders
		}
		sender := senders[message.To]
		if sender.queue == nil {
			retired, err := n.memberRetired(message.To)
			if err != nil || retired {
				n.peerMu.Unlock()
				if snapshotFile != nil {
					snapshotFile.Close()
				}
				if err != nil {
					n.fail(err)
					return
				}
				continue
			}
			ctx, cancel := context.WithCancel(n.ctx)
			sender = peerSender{queue: make(chan packet, 128), ctx: ctx, cancel: cancel}
			senders[message.To] = sender
			n.workers.Add(1)
			go n.sendLoop(message.To, sender)
		}
		if sender.ctx.Err() != nil {
			n.peerMu.Unlock()
			if snapshotFile != nil {
				snapshotFile.Close()
			}
			continue
		}
		queued := false
		select {
		case sender.queue <- packet{data: data, snapshot: message.Type == raftpb.MsgSnap, control: control, file: snapshotFile}:
			queued = true
		default:
		}
		n.peerMu.Unlock()
		if !queued {
			n.metrics.Event("transport_queue_full")
			if snapshotFile != nil {
				snapshotFile.Close()
			}
			n.raft.ReportUnreachable(message.To)
			if message.Type == raftpb.MsgSnap {
				n.raft.ReportSnapshot(message.To, raft.SnapshotFailure)
			}
		}
	}
}

func (n *Node) sendLoop(id uint64, sender peerSender) {
	queue := sender.queue
	defer n.workers.Done()
	defer func() {
		for {
			select {
			case packet := <-queue:
				if packet.file != nil {
					packet.file.Close()
				}
			default:
				return
			}
		}
	}()
	for {
		select {
		case <-sender.ctx.Done():
			return
		case packet := <-queue:
			if sender.ctx.Err() != nil {
				if packet.file != nil {
					packet.file.Close()
				}
				return
			}
			operation := "message_send"
			if packet.snapshot {
				operation = "snapshot_send"
			} else if packet.control {
				operation = "control_send"
			}
			finish := n.metrics.Start(operation)
			n.peerMu.RLock()
			address := n.peers[id]
			n.peerMu.RUnlock()
			err := func() error {
				if packet.file != nil {
					defer packet.file.Close()
				}
				if address == "" {
					return fmt.Errorf("no address for Raft peer %d", id)
				}
				var body io.Reader = bytes.NewReader(packet.data)
				var contentLength int64
				endpoint := "/raft/message"
				if packet.file != nil {
					peer, err := n.peerStatus(sender.ctx, id)
					if err != nil {
						return err
					}
					if !n.cfg.StreamSnapshots || peer.SnapshotFormat < 2 {
						var message raftpb.Message
						if err := message.Unmarshal(packet.data); err != nil {
							return err
						}
						body, contentLength, err = inlineSnapshotBody(message, packet.file, n.cfg.MaxSnapshotBytes)
						if err != nil {
							return err
						}
					} else {
						var prefix [4]byte
						binary.BigEndian.PutUint32(prefix[:], uint32(len(packet.data)))
						body = io.MultiReader(bytes.NewReader(prefix[:]), bytes.NewReader(packet.data), packet.file)
						endpoint = "/raft/snapshot"
					}
				}
				request, err := http.NewRequestWithContext(sender.ctx, http.MethodPost, strings.TrimRight(address, "/")+endpoint, body)
				if err != nil {
					return err
				}
				if contentLength > 0 {
					request.ContentLength = contentLength
				}
				request.Header.Set("Authorization", "Bearer "+n.cfg.Token)
				request.Header.Set("X-Raft-Cluster", n.cfg.ClusterID)
				request.Header.Set(protocolHeader, fmt.Sprint(ProtocolVersion))
				if endpoint == "/raft/snapshot" {
					request.Header.Set(snapshotStreamHeader, "2")
				}
				client := n.client.Client
				if packet.snapshot {
					streamClient := *client
					streamClient.Timeout = 10 * time.Minute
					client = &streamClient
				} else if packet.control && n.controlClient.Client != nil {
					client = n.controlClient.Client
				}
				response, err := client.Do(request)
				if err != nil {
					return err
				}
				defer response.Body.Close()
				io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
				if response.StatusCode != http.StatusNoContent {
					return fmt.Errorf("peer %d returned %d", id, response.StatusCode)
				}
				if !n.protocolCompatible(response.Header.Get(protocolHeader)) {
					return fmt.Errorf("peer %d has an incompatible Raft protocol", id)
				}
				return nil
			}()
			finish(err)
			if err != nil {
				n.raft.ReportUnreachable(id)
			}
			if packet.snapshot {
				status := raft.SnapshotFinish
				if err != nil {
					status = raft.SnapshotFailure
				}
				n.raft.ReportSnapshot(id, status)
			}
		}
	}
}

func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/snapshot", n.receiveSnapshot)
	mux.HandleFunc("POST /raft/snapshot/probe", n.snapshotProbeHandler)
	mux.HandleFunc("POST /raft/message", func(w http.ResponseWriter, r *http.Request) {
		if !n.protocolCompatible(r.Header.Get(protocolHeader)) {
			http.Error(w, "incompatible Raft protocol", http.StatusUpgradeRequired)
			return
		}
		const appendLimit = MaxProposalBytes + (32 << 20)
		limit := max(inlineSnapshotLimit(n.cfg.MaxSnapshotBytes)+(32<<20), appendLimit)
		if r.ContentLength > limit {
			http.Error(w, "Raft message exceeds the receive byte limit", http.StatusRequestEntityTooLarge)
			return
		}
		// Compatibility snapshots can be much larger than append messages.
		// Admit only one large or unframed receive without occupying a slot
		// for normal replication and quorum heartbeats.
		large := r.ContentLength < 0 || r.ContentLength > 2<<20
		if n.receiveLargeMessage != nil && large {
			select {
			case n.receiveLargeMessage <- struct{}{}:
				defer func() { <-n.receiveLargeMessage }()
			default:
				http.Error(w, "large Raft message receiver is busy", http.StatusServiceUnavailable)
				return
			}
		}
		var body io.Reader = http.MaxBytesReader(w, r.Body, limit)
		_, streaming := n.machine.(StreamingStateMachine)
		if streaming && large {
			reader := bufio.NewReaderSize(body, 32<<10)
			body = reader
			// GraphDB's raftpb encoders put the message type first.
			if tag, _ := reader.Peek(2); len(tag) == 2 && tag[0] == 1<<3 && tag[1] == byte(raftpb.MsgSnap) {
				n.receiveInlineSnapshot(w, r, reader)
				return
			}
			body = io.LimitReader(reader, appendLimit+1)
		}
		data, err := io.ReadAll(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		if streaming && int64(len(data)) > appendLimit {
			http.Error(w, "Raft message exceeds the append byte limit", http.StatusRequestEntityTooLarge)
			return
		}
		var message raftpb.Message
		if err := message.Unmarshal(data); err != nil {
			http.Error(w, "invalid Raft message", http.StatusBadRequest)
			return
		}
		if len(data) > appendLimit && message.Type != raftpb.MsgSnap {
			http.Error(w, "Raft message exceeds the append byte limit", http.StatusRequestEntityTooLarge)
			return
		}
		n.peerMu.RLock()
		_, known := n.peers[message.From]
		n.peerMu.RUnlock()
		if !known || message.To != n.cfg.ID || message.From == n.cfg.ID || raft.IsLocalMsg(message.Type) {
			http.Error(w, "invalid Raft peer message", http.StatusBadRequest)
			return
		}
		if err := n.available(false); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		if n.draining.Load() && message.Type == raftpb.MsgTimeoutNow {
			http.Error(w, "drained replica cannot accept leadership", http.StatusServiceUnavailable)
			return
		}
		if message.Type == raftpb.MsgSnap && message.Snapshot != nil {
			if message.Snapshot.Metadata.Index == 0 {
				http.Error(w, "invalid inline snapshot index", http.StatusBadRequest)
				return
			}
			if message.Snapshot.Metadata.Index <= n.committed.Load() || message.Snapshot.Metadata.Index <= n.snapshotIndex.Load() {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			envelope, err := decodeSnapshot(message.Snapshot.Data)
			if err != nil || envelope.Version != 1 || int64(len(envelope.State)) > n.cfg.MaxSnapshotBytes || int64(len(message.Snapshot.Data)) > inlineSnapshotLimit(n.cfg.MaxSnapshotBytes) {
				http.Error(w, "invalid inline snapshot envelope", http.StatusBadRequest)
				return
			}
			if _, ok := n.machine.(StreamingStateMachine); ok {
				if n.cfg.SnapshotPreflight != nil {
					if err := n.cfg.SnapshotPreflight(r.Context(), int64(len(envelope.State))); err != nil {
						http.Error(w, err.Error(), http.StatusServiceUnavailable)
						return
					}
				}
				staged, stored, err := stageSnapshotFile(n.cfg.Dir, bytes.NewReader(envelope.State), nil, n.cfg.MaxSnapshotBytes)
				if err != nil {
					http.Error(w, "persist inline snapshot failed", http.StatusServiceUnavailable)
					return
				}
				defer func() {
					_ = staged.Close()
					_ = os.Remove(staged.Name())
				}()
				envelope.Version, envelope.State, envelope.File = 2, nil, &stored
				message.Snapshot.Data, err = json.Marshal(envelope)
				if err != nil {
					http.Error(w, "encode snapshot metadata failed", http.StatusInternalServerError)
					return
				}
				if err := n.stepReceivedSnapshot(r.Context(), message, staged, stored); err != nil {
					http.Error(w, err.Error(), http.StatusServiceUnavailable)
					return
				}
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		if err := n.raft.Step(r.Context(), message); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /raft/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		status := n.Status()
		n.peerMu.RLock()
		peers := make(map[uint64]string, len(n.peers))
		for id, origin := range n.peers {
			peers[id] = origin
		}
		n.peerMu.RUnlock()
		status["peers"] = peers
		json.NewEncoder(w).Encode(status)
	})
	mux.HandleFunc("POST /raft/members", n.changeMember)
	n.rollingHandlers(mux)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+n.cfg.Token)) != 1 || r.Header.Get("X-Raft-Cluster") != n.cfg.ClusterID {
			http.Error(w, "unauthorized Raft request", http.StatusUnauthorized)
			return
		}
		w.Header().Set(protocolHeader, fmt.Sprint(ProtocolVersion))
		mux.ServeHTTP(w, r)
	})
}

func (n *Node) loadPeers() error {
	return n.disk.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket([]byte("meta")).Get([]byte("peers"))
		if data == nil {
			return nil
		}
		return json.Unmarshal(data, &n.peers)
	})
}

func (n *Node) LeaderID() uint64                { return n.leader.Load() }
func (n *Node) ID() uint64                      { return n.cfg.ID }
func (n *Node) Check(ctx context.Context) error { return n.ReadBarrier(ctx) }

func (n *Node) installPeers(peers map[uint64]string) error {
	data, err := json.Marshal(peers)
	if err != nil {
		return err
	}
	if err := n.disk.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("meta")).Put([]byte("peers"), data) }); err != nil {
		return err
	}
	n.peerMu.Lock()
	defer n.peerMu.Unlock()
	for id, address := range peers {
		n.peers[id] = address
	}
	return nil
}
