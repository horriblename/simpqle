## Concepts

### Pager

Rows are grouped into pages, and data is stored/retrieved from disk in the unit
of pages.

### B+ Tree

Brief description of a B+ Tree:

- self balancing tree
- each tree consists of a **root**, **internal nodes** and **leaves**
  - internal nodes are "branches"
- root may be either a leaf or a node with two or more children
- each node contains up to N cells
- each **internal** node stores up to N **keys**
- each **leaf** node stores up to N **key value pairs**

In our impl, each node corresponds to one page. The root node will exist in page 0. Child pointers will simply be the page number that contains the child node
