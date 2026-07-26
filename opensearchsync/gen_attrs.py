#!/usr/bin/env python3
# Generates attrs.go from condor_adstash's convert.py attribute tables.
# Usage: python3 gen_attrs.py /path/to/htcondor/src/condor_scripts/adstash/convert.py > attrs.go
import ast, sys

import sys
convert_py = sys.argv[1] if len(sys.argv) > 1 else "convert.py"
src = open(convert_py).read()
mod = ast.parse(src)

SETS = ["REQUIRED_ATTRS","INDEXED_KEYWORD_ATTRS","NOINDEX_KEYWORD_ATTRS","FLOAT_ATTRS",
        "INT_ATTRS","DATE_ATTRS","BOOL_ATTRS","NESTED_ATTRS","IGNORE_ATTRS"]
DICTS = ["STATUS","UNIVERSE"]

setvals, dictvals = {}, {}
for node in mod.body:
    if not isinstance(node, ast.Assign): continue
    for t in node.targets:
        if not isinstance(t, ast.Name): continue
        name = t.id
        if name in SETS:
            vals=[]
            v=node.value
            # handle `{} or set()` -> empty
            if isinstance(v, ast.BoolOp):
                setvals[name]=[]
                continue
            if isinstance(v, ast.Set):
                for e in v.elts:
                    if isinstance(e, ast.Constant): vals.append(e.value)
            setvals[name]=vals
        elif name in DICTS:
            pairs=[]
            if isinstance(node.value, ast.Dict):
                for k,val in zip(node.value.keys, node.value.values):
                    if isinstance(k, ast.Constant) and isinstance(val, ast.Constant):
                        pairs.append((k.value, val.value))
            dictvals[name]=pairs

def goname(py):
    m={"REQUIRED_ATTRS":"requiredAttrs","INDEXED_KEYWORD_ATTRS":"indexedKeywordAttrs",
       "NOINDEX_KEYWORD_ATTRS":"noindexKeywordAttrs","FLOAT_ATTRS":"floatAttrs",
       "INT_ATTRS":"intAttrs","DATE_ATTRS":"dateAttrs","BOOL_ATTRS":"boolAttrs",
       "NESTED_ATTRS":"nestedAttrs","IGNORE_ATTRS":"ignoreAttrs"}
    return m[py]

out=[]
out.append("// Code generated from condor_adstash convert.py attribute tables. DO NOT EDIT BY HAND.")
out.append("// Regenerate with scratchpad/gen_attrs.py against the HTCondor source tree.")
out.append("")
out.append("package opensearchsync")
out.append("")
for name in SETS:
    vals=setvals.get(name,[])
    out.append(f"// {name.lower()} ({len(vals)} attrs)")
    out.append(f"var {goname(name)} = []string{{")
    for v in vals:
        out.append(('\t%r,' % (v,)).replace(chr(39), chr(34)))
    out.append("}")
    out.append("")
for name in DICTS:
    pairs=dictvals.get(name,[])
    gn = "statusNames" if name=="STATUS" else "universeNames"
    out.append(f"var {gn} = map[int64]string{{")
    for k,v in pairs:
        out.append(f'\t{k}: "{v}",')
    out.append("}")
    out.append("")

sys.stdout.write("\n".join(out))
