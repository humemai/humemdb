# Grammar Inputs

This directory will hold parser-facing grammar inputs for HumemDB's Cypher frontend.

Initial plan:

1. start from a parser-ready ANTLR grammar artifact for openCypher 9
2. keep the cloned openCypher main repo as BNF/TCK reference material
3. avoid coupling the runtime directly to an external parser package API
