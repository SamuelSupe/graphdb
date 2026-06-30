import os
import unittest

from graphdb_sdk import GraphDBAPIError, GraphDBClient


class RealServerE2ETest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base_url = os.environ.get("GRAPHDB_TEST_BASE_URL", "")
        cls.tenant_id = os.environ.get("GRAPHDB_TEST_TENANT", "python-sdk-e2e")
        if not cls.base_url:
            raise unittest.SkipTest("GRAPHDB_TEST_BASE_URL is not set")
        cls.client = GraphDBClient(cls.base_url, tenant_id=cls.tenant_id, timeout=5)

    def test_python_sdk_complete_flow_against_real_server(self):
        c = self.client
        tenant = self.tenant_id

        self.assertEqual(c.health()["status"], "ok")
        created = c.create_tenant(tenant, name="Python SDK E2E")
        self.assertEqual(created["tenant_id"], tenant)
        self.assertEqual(c.get_tenant(tenant)["status"], "active")
        self.assertTrue(any(item["tenant_id"] == tenant for item in c.list_tenants()["tenants"]))

        policy = {
            "default_priority": 0,
            "sources": [
                {"name": "manual", "priority": 1000},
                {"name": "agent", "priority": 100},
            ],
        }
        self.assertEqual(c.put_source_policy(policy)["policy"]["sources"][0]["name"], "manual")
        self.assertTrue(c.get_source_policy()["configured"])
        self.assertTrue(c.put_tenant_config({"retention": {"keep_snapshots": 2}})["configured"])
        self.assertTrue(c.get_tenant_config()["configured"])

        mutations = {
            "upsert_ci_types": [
                {"name": "host", "fields": {"hostname": {"type": "string", "indexed": True}}},
                {"name": "service"},
            ],
            "upsert_relation_types": [
                {"name": "runs_on", "from_kind": "service", "to_kind": "host", "directed": True},
            ],
            "upsert_entities": [
                {
                    "id": "host:py",
                    "kind": "host",
                    "source": "agent",
                    "source_priority": 100,
                    "fields": {"hostname": "py-host", "cpu": 8},
                },
                {
                    "id": "service:py",
                    "kind": "service",
                    "source": "manual",
                    "source_priority": 1000,
                    "fields": {"name": "py-service"},
                },
            ],
            "upsert_edges": [
                {
                    "id": "edge:py-runs-on",
                    "type": "runs_on",
                    "from": "service:py",
                    "to": "host:py",
                    "source": "manual",
                    "source_priority": 1000,
                    "fields": {"status": "active"},
                }
            ],
        }
        commit = c.commit(mutations, idempotency_key="py-sdk-e2e-commit")
        self.assertGreaterEqual(commit["version"], 1)
        version = commit["version"]

        entity = c.get_entity("host:py", min_version=version)
        self.assertEqual(entity["fields"]["hostname"], "py-host")
        self.assertEqual(c.list_entities(kind="host", limit=10)["entities"][0]["id"], "host:py")
        self.assertEqual(c.list_edges(type="runs_on", limit=10)["edges"][0]["to"], "host:py")
        with c.stream_entities(kind="host", limit=10) as stream:
            self.assertEqual([item["entity"]["id"] for item in stream if "entity" in item], ["host:py"])
        with c.stream_edges(type="runs_on", limit=10) as stream:
            self.assertEqual(len([item for item in stream if "edge" in item]), 1)

        match = c.query({"op": "match", "kind": "host", "filters": {"hostname": "py-host"}, "limit": 10})
        self.assertEqual(match["results"][0]["entity"]["id"], "host:py")
        gql = c.gql('FIND host WHERE hostname = "py-host" LIMIT 10')
        self.assertEqual(gql["results"][0]["entity"]["id"], "host:py")
        with c.stream_query({"op": "match", "kind": "host", "limit": 10}) as stream:
            self.assertTrue(any(item.get("entity", {}).get("id") == "host:py" for item in stream))
        with c.stream_gql("FIND host LIMIT 10") as stream:
            self.assertTrue(any(item.get("entity", {}).get("id") == "host:py" for item in stream))

        saved = c.save_query(
            "py-host-by-name",
            {"op": "match", "kind": "host", "filters": {"hostname": "py-host"}, "limit": 1},
        )
        self.assertEqual(saved["name"], "py-host-by-name")
        self.assertTrue(any(item["name"] == "py-host-by-name" for item in c.list_queries()["queries"]))
        self.assertEqual(c.run_saved_query("py-host-by-name")["results"][0]["entity"]["id"], "host:py")
        self.assertIn("queries", c.list_running_queries())

        self.assertIn("version", c.rebuild_indexes(async_=False))
        self.assertIn("status", c.index_health())
        self.assertIn("version", c.rebuild_indexes(async_=False))
        self.assertIn("visible_version", c.reader_freshness())
        self.assertIn("owner_id", c.writer_lease())
        self.assertIn("status", c.integrity_audit(deep=False))
        self.assertIn("status", c.repair(apply=False))

        task = c.start_task("export_snapshot")
        self.assertEqual(task["type"], "export_snapshot")
        self.assertEqual(c.get_task(task["id"])["id"], task["id"])
        self.assertTrue(any(item["id"] == task["id"] for item in c.list_tasks()["tasks"]))

        ingest = c.ingest(
            {
                "source": "agent",
                "collector_id": "python-sdk",
                "batch_id": "py-batch-1",
                "idempotency_key": "py-batch-1",
                "items": [
                    {
                        "external_id": "host-py-2",
                        "entity": {
                            "id": "host:py-2",
                            "kind": "host",
                            "fields": {"hostname": "py-host-2"},
                        },
                    }
                ],
            }
        )
        self.assertEqual(ingest["failed"], 0)
        self.assertEqual(c.get_entity("host:py-2")["fields"]["hostname"], "py-host-2")

        disabled = c.disable_tenant(tenant)
        self.assertEqual(disabled["status"], "disabled")
        with self.assertRaises(GraphDBAPIError) as caught:
            c.commit({"upsert_entities": [{"id": "host:blocked", "kind": "host"}]})
        self.assertEqual(caught.exception.status_code, 403)
        self.assertEqual(c.enable_tenant(tenant)["status"], "active")

        clone = c.clone_tenant(tenant, tenant + "-clone")
        self.assertEqual(clone["tenant_id"], tenant + "-clone")
        self.assertEqual(c.for_tenant(tenant + "-clone").get_entity("host:py")["id"], "host:py")


if __name__ == "__main__":
    unittest.main()
