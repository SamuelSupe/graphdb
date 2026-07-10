#!/usr/bin/env bash

show_help() {
      echo "支持的选项: "
      echo "  -t  打测试镜像"
      echo "  -p  打预发镜像"
      echo "  -r  打生产发布镜像"
      echo "  -f  在最后的生产版本上打一个 Bug 修复镜像"
}

auto_tag() {
  lastTag=$(git tag --list | grep -E "^${1}" | sort -V | tail -1)

  if [ ${#lastTag} -gt 0 ]; then
    read -r -a v <<< "${lastTag//_/ }"
    v_main=${v[0]}_${v[1]}

    # 当前版本的 tag 已经存在，后续版本数字递增1
    releaseCount=$((10#${lastTag:${#v_main}+1:2} + 101))
    releaseCount=${releaseCount:1:2}

    newTag=${v_main}_${releaseCount}
  else
    newTag=${1}_01
  fi

  newTag=${newTag//[\.\/]/_}
  git tag "$newTag"
  git push && git push --tag

  echo "$newTag"
}

if [ $# -gt 0 ]; then
  if [ "$1" == "--help" ] || [ "$1" == "-h" ] || [ "$1" == "help" ]; then
    show_help
    exit 0
  fi
fi

git fetch --tag

while getopts ":ftpr" opt; do
  case ${opt} in
  f)
    lastReleaseTag=$(git tag --list | grep -E "^release_" | sort -V | tail -1)
    auto_tag "$lastReleaseTag"
    ;;
  t)
    auto_tag testing_"$(date +%Y%m%d)"
    ;;
  p)
    auto_tag pre_"$(date +%Y%m%d)"
    ;;
  r)
    auto_tag release_"$(date +%Y%m%d)"
    ;;
  \?)
    show_help
    break
    ;;
  esac
done
