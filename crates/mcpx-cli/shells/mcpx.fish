function __mcpx_complete
    command mcpx __complete fish --line (commandline --current-process --cut-at-cursor) 2>/dev/null
end
complete -c mcpx -f -a '(__mcpx_complete)'
