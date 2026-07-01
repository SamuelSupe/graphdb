#!/usr/bin/env bash
set -euo pipefail

die() {
  echo "Error: $*" >&2
  exit 1
}

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    die "$name is required"
  fi
}

latest_tag() {
  local pattern="$1"
  git tag --list | grep -E "$pattern" | sort -V | tail -1 || true
}

get_rancher_config() {
  local cluster_with_prefix="$1"
  local prefix site cluster_id api_url="" rancher_token="" item

  IFS=':' read -r prefix site cluster_id <<< "$cluster_with_prefix"
  if [[ -z "$prefix" || -z "$cluster_id" ]]; then
    echo "Invalid cluster entry: $cluster_with_prefix, want <prefix>:<site>:<cluster-id>" >&2
    return 1
  fi

  require_env RANCHER_API_BASE_URL
  require_env PROD_RANCHER_TOKEN

  for item in ${RANCHER_API_BASE_URL}; do
    if [[ "$item" == "${prefix}:"* ]]; then
      api_url="${item#*:}"
      break
    fi
  done

  for item in ${PROD_RANCHER_TOKEN}; do
    if [[ "$item" == "${prefix}:"* ]]; then
      rancher_token="${item#*:}"
      break
    fi
  done

  if [[ -z "$api_url" || -z "$rancher_token" ]]; then
    echo "No Rancher configuration found for prefix $prefix" >&2
    return 1
  fi

  printf '%s\t%s\t%s\n' "$api_url" "$rancher_token" "$cluster_id"
}

do_cd() {
  local cluster_with_prefix="$1"
  local image_full_name="$2"
  local namespace="$3"
  local workload="$4"
  local version="$5"
  local workload_type="$6"
  local rancher_config api_url token cluster_id url image_path data

  if ! rancher_config="$(get_rancher_config "$cluster_with_prefix")"; then
    return 1
  fi

  IFS=$'\t' read -r api_url token cluster_id <<< "$rancher_config"
  url="${api_url}/k8s/clusters/${cluster_id}/apis/apps/v1/namespaces/${namespace}/${workload_type}/${workload}"
  image_path="${image_full_name}:${version}"
  printf -v data '[{"op":"replace","path":"/spec/template/spec/containers/0/image","value":"%s"}]' "$image_path"

  echo "Patching ${workload_type}/${workload} to ${image_path}"
  curl -fsS -u "$token" -X PATCH -H 'Content-Type: application/json-patch+json' "$url" -d "$data"
}

deploy_cluster() {
  local cluster_with_prefix="$1"
  local version="$2"
  local image_full_name workload deployed=0

  require_env DEPLOY_IMAGE_HOST
  require_env DEPLOY_IMAGE_PATH
  require_env DEPLOY_NAMESPACE

  echo "do upgrade, version: ${version}"
  image_full_name="${DEPLOY_IMAGE_HOST}${DEPLOY_IMAGE_PATH}"

  for workload in ${DEPLOY_PROJECT_WORKLOAD:-}; do
    deployed=1
    do_cd "$cluster_with_prefix" "$image_full_name" "$DEPLOY_NAMESPACE" "$workload" "$version" deployments
  done

  for workload in ${DEPLOY_PROJECT_STATEFULSET:-}; do
    deployed=1
    do_cd "$cluster_with_prefix" "$image_full_name" "$DEPLOY_NAMESPACE" "$workload" "$version" statefulsets
  done

  if [[ "$deployed" -eq 0 ]]; then
    die "DEPLOY_PROJECT_WORKLOAD or DEPLOY_PROJECT_STATEFULSET must include at least one workload"
  fi
}

each_cluster() {
  local last_release_tag last_deploy_tag split_site="" cluster_with_prefix prefix site cluster_id

  require_env RANCHER_CLUSTER_IDS

  git fetch --tags
  last_release_tag="$(latest_tag "^release_")"
  last_deploy_tag="$(latest_tag "^deploy_")"

  if [[ -z "$last_release_tag" ]]; then
    die "No release_* tag found"
  fi

  if [[ -n "$last_deploy_tag" ]]; then
    IFS='_' read -r _ _ split_site _ <<< "$last_deploy_tag"
  fi

  for cluster_with_prefix in ${RANCHER_CLUSTER_IDS}; do
    IFS=':' read -r prefix site cluster_id <<< "$cluster_with_prefix"
    if [[ -z "$split_site" || "$split_site" == "all" || "$site" == "$split_site" ]]; then
      echo "Deploying to cluster: $cluster_with_prefix with version: $last_release_tag"
      deploy_cluster "$cluster_with_prefix" "$last_release_tag"
    fi
  done
}

do_trigger_tag() {
  local trigger_site="$1"
  local last_release_tag last_release_commit last_deploy_tag new_deploy_tag deploy_count deploy_prefix

  git fetch --tags
  last_release_tag="$(latest_tag "^release_")"
  last_deploy_tag="$(latest_tag "^deploy_")"

  if [[ -z "$last_release_tag" ]]; then
    die "No release_* tag found"
  fi

  last_release_commit="$(git rev-list -n 1 "$last_release_tag")"

  if [[ -n "$last_deploy_tag" ]]; then
    deploy_prefix="${last_deploy_tag%%_*}"
    deploy_count=$((10#${last_deploy_tag:${#deploy_prefix} + 1:5} + 100001))
    deploy_count="${deploy_count:1:5}"
  else
    deploy_prefix="deploy"
    deploy_count="00001"
  fi

  if [[ -z "$trigger_site" || "$trigger_site" == "all" ]]; then
    new_deploy_tag="${deploy_prefix}_${deploy_count}"
  else
    new_deploy_tag="${deploy_prefix}_${deploy_count}_${trigger_site}"
  fi

  echo "$new_deploy_tag"
  git tag -a "$new_deploy_tag" -m "deploy trigger ${new_deploy_tag}" "$last_release_commit"
  git push --tags
}

usage() {
  echo "Usage:"
  echo "  bash cd.sh -d"
  echo "  bash cd.sh -t <site|all>"
}

if [[ "$#" -eq 0 ]]; then
  usage
  exit 1
fi

while getopts ":t:d" opt; do
  case "$opt" in
    t)
      echo "add a tag to deploy trigger"
      do_trigger_tag "$OPTARG"
      ;;
    d)
      echo "do deploy"
      each_cluster
      ;;
    :)
      die "Option -$OPTARG requires an argument"
      ;;
    ?)
      usage
      exit 1
      ;;
  esac
done
