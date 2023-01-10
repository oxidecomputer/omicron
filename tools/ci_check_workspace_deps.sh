#!/bin/bash
set -u

exit=0

# Check all workspace members
readarray -t workspace_crates < <(toml get Cargo.toml workspace.members | jq -r '.[]')

# For all the non-workspace dependencies, grab their non-workspace dependencies
# and build a reverse dependency map
declare -A crate_deps
declare -A reverse_crate_deps
for crate_path in "${workspace_crates[@]}"; do
    crate=$(toml get -r $crate_path/Cargo.toml package.name)

    # Grab all the non-workspace dependencies of this crate
    readarray -t non_workspace_deps < <(
        {
            toml get $crate_path/Cargo.toml dependencies
            toml get $crate_path/Cargo.toml build-dependencies
            toml get $crate_path/Cargo.toml dev-dependencies
        } | jq -r 'to_entries | map(select((.value.workspace? // false) != true)) | .[].key'
    )
    crate_deps[$crate]="${non_workspace_deps[@]}"

    for dep in "${non_workspace_deps[@]}"; do
        if [ -z "${reverse_crate_deps[$dep]:-}" ]; then
            reverse_crate_deps[$dep]="$crate"
        else
            reverse_crate_deps[$dep]="${reverse_crate_deps[$dep]} $crate"
        fi
    done
done

# Grab all the top-level workspace dependencies
readarray -t workspace_deps < <(toml get Cargo.toml workspace.dependencies | jq -r 'keys[]')

# Check every workspace crate for shared non-workspace dependencies
for crate in "${!crate_deps[@]}"; do

    readarray -t deps <<<"${crate_deps[$crate]}"
    for dep in $deps; do
        # Check if any of the non-workspace dependencies are already workspace dependencies
        if [[ " ${workspace_deps[@]} " =~ " $dep " ]]; then
            echo "$dep exists as workspace dependency but also used as non-workspace dependency in $crate"
            exit=1
        # Ignore pq-sys and omicron-rpaths because they're special
        elif [ "$dep" != "pq-sys" ] && [ "$dep" != "omicron-rpaths" ]; then
            # Check if we're the only crate that depends on this dependency
            if [ " ${reverse_crate_deps[$dep]} " != " $crate " ]; then
                echo "$dep used by multiple crates (${reverse_crate_deps[$dep]}), but not a workspace dependency"
                exit=1
            fi
        fi
    done
done

exit $exit
