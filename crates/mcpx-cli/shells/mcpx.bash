# Dynamic MCP completion; source this file after installing mcpx on PATH.
_mcpx_complete() {
    local candidate
    local prefix=${COMP_LINE:0:COMP_POINT}
    COMPREPLY=()
    builtin compopt +o nospace 2>/dev/null || true
    while IFS= read -r candidate; do
        COMPREPLY+=("$candidate")
        [[ $candidate != */ ]] || builtin compopt -o nospace 2>/dev/null || true
    done < <(command mcpx __complete bash --line "$prefix" --current "${COMP_WORDS[COMP_CWORD]}" --fragment "${2-}" 2>/dev/null)
}
complete -F _mcpx_complete mcpx
