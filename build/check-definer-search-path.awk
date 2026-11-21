# Check every routine declaration in the final installer, including overrides.
# Mask comments, quoted values and dollar-quoted bodies before reading attributes.
# PgQue declarations put SET search_path last; reject a different form for review.
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

function check_statement(    normalized, path) {
    normalized = tolower(statement)
    if (normalized !~ /^[[:space:]]*create[[:space:]]+(or[[:space:]]+replace[[:space:]]+)?(function|procedure)[[:space:]]/)
        return
    if (normalized !~ /security[[:space:]]+definer/)
        return
    definers++
    if (!match(normalized, /set[[:space:]]+search_path[[:space:]]*(=|to)[[:space:]]*/)) {
        printf "FAIL: definer declaration at line %d has no SET search_path\n", statement_line > "/dev/stderr"
        errors++
        return
    }
    path = substr(normalized, RSTART + RLENGTH)
    gsub(/[[:space:]]/, "", path)
    if (path != "pgque,pg_catalog,pg_temp") {
        printf "FAIL: definer declaration at line %d must end with SET search_path = pgque, pg_catalog, pg_temp\n", statement_line > "/dev/stderr"
        errors++
    }
}

{
    for (i = 1; i <= length($0); i++) {
        ch = substr($0, i, 1)
        pair = substr($0, i, 2)
        if (dollar_tag != "") {
            if (substr($0, i, length(dollar_tag)) == dollar_tag) {
                i += length(dollar_tag) - 1
                dollar_tag = ""
            }
            continue
        }
        if (comment_depth) {
            if (pair == "/*") { comment_depth++; i++ }
            else if (pair == "*/") { comment_depth--; i++ }
            continue
        }
        if (quote != "") {
            if (escape_string && ch == "\\") { i++; continue }
            if (ch == quote) {
                if (substr($0, i + 1, 1) == quote) i++
                else quote = ""
            }
            continue
        }
        if (pair == "--") break
        if (pair == "/*") {
            comment_depth = 1
            statement = statement " "
            i++
            continue
        }
        if (ch == "\047" || ch == "\042") {
            quote = ch
            escape_string = ch == "\047" && statement ~ /(^|[^[:alnum:]_])[eE]$/
            statement = statement " "
            continue
        }
        if (ch == "$" && match(substr($0, i), /^\$([[:alpha:]_][[:alnum:]_]*)?\$/)) {
            dollar_tag = substr($0, i, RLENGTH)
            i += RLENGTH - 1
            statement = statement " "
            continue
        }
        if (ch == ";") {
            check_statement()
            statement = ""
            statement_line = 0
        } else {
            if (!statement_line && ch !~ /[[:space:]]/) statement_line = NR
            statement = statement ch
        }
    }
    statement = statement " "
}

END {
    if (quote != "" || dollar_tag != "" || comment_depth) {
        print "FAIL: unfinished quoted value or comment in installer" > "/dev/stderr"
        errors++
    }
    if (!definers) {
        print "FAIL: final installer contains no SECURITY DEFINER declarations" > "/dev/stderr"
        errors++
    }
    if (errors) exit 1
    printf "PASS: all %d final-assembly definer declarations explicitly place pg_temp last\n", definers
}
