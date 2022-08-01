#!/bin/bash
NPD_CUSTOM_PLUGINS_VERSION="${NPD_CUSTOM_PLUGINS_VERSION:-v1.0.6}"
NPD_CUSTOM_PLUGINS_TAR_HASH="${NPD_CUSTOM_PLUGINS_TAR_HASH:-fde65e01607fcd037136e82f71b8b4f5f931ea23752fa5621a75f23d43435fb9ce5d4f9e3ab33a14789919b2977eda34886d166b6aa4dd381c6de056ed7d6917}"
NPD_CUSTOM_PLUGINS_RELEASE_PATH="${NPD_CUSTOM_PLUGINS_RELEASE_PATH:-https://storage.googleapis.com/gke-release}"

M4A_APPARMOR_PROFILE_HASH="${M4A_APPARMOR_PROFILE_HASH:-cd84b52e756bee90b4a26612b372519ebf942bb3b6145d1928d3c1ae0faa4a17ea040f3e5f0429df9193dfcf84364d6a4ac56ebefb70420ae12579be5c5b5756}"
M4A_APPARMOR_RELEASE_PATH="${M4A_APPARMOR_RELEASE_PATH:-https://storage.googleapis.com/anthos-migrate-release}"
# Install node problem detector custom plugins.
function install-npd-custom-plugins {
  local -r version="${NPD_CUSTOM_PLUGINS_VERSION}"
  local -r hash="${NPD_CUSTOM_PLUGINS_TAR_HASH}"
  local -r release_path="${NPD_CUSTOM_PLUGINS_RELEASE_PATH}"
  local -r tar="npd-custom-plugins-${version}.tar.gz"

  echo "Downloading ${tar}."
  download-or-bust "${hash}" "${release_path}/npd-custom-plugins/${version}/${tar}"
  local -r dir="${KUBE_HOME}/npd-custom-plugins"
  mkdir -p "${dir}"
  tar xzf "${KUBE_HOME}/${tar}" -C "${dir}" --overwrite
}

function record-preload-info {
  echo "$1,$2" >> "${KUBE_HOME}/preload_info"
}

function is-preloaded {
  local -r key=$1
  local -r value=$2
  grep -qs "${key},${value}" "${KUBE_HOME}/preload_info"
}

function install-m4a-apparmor-profile {
  local -r hash="${M4A_APPARMOR_PROFILE_HASH}"
  local -r release_path="${M4A_APPARMOR_RELEASE_PATH}"
  local -r profile="m4a-apparmor-profile"

  if is-preloaded "${profile}" "${hash}"; then
    echo "m4a apparmor profile is preloaded."
    return
  fi

  if type apparmor_parser; then
    echo "Downloading ${profile}."
    if ! [ download-or-bust "${hash}" "${release_path}/artifacts/${profile}" ]; then
      echo "Failed to download ${profile}, cannot install M4A apparmor profile"
      return
    fi

    # This call is expected to fail as the profile is likely not installed.
    # we are only using this as a safety measure against changes in Ubuntu
    # image and  installing a clean one
    sudo apparmor_parser --remove ${KUBE_HOME}/${profile} > /dev/null 2>&1 || true
    sudo apparmor_parser ${KUBE_HOME}/${profile}
    record-preload-info "${profile}" "${hash}"
  else
    echo "No apparmor_parser found, cannot install M4A apparmor profile"
  fi
}
