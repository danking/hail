Designers recommend drafting a *user flow* before you build a website. The user
flow should define who is your anticipated audience (or audiences) and what do
they want to achieve. I believe we have four primary audiences (everything in
this document is up for discussion):

The first audience is ACBs, grad students, and post-docs, i.e. the do-ers. These
folks land at the Hail website for one of three reasons: they want to install
Hail, they encountered unexpected (to them) behavior from Hail, or they want to
know if and how Hail can do some genetics analysis.

The second audience is PIs, i.e. the ideas and money people. These folks land at
the Hail website because a paper or colleague mentioned it. They want to quickly
determine of what Hail is capable. They are particularly interested in
high-level assertions that it will save time or money. They will probably not
make any decisions, but may ask a do-er to investigate further.

The third audience is people with a software engineering background. These folks
might be SWEs at biopharmas or maybe ACBs with CS backgrounds. They have heard
of pandas, numpy, and Spark. They want to know how Hail fits into their mental
model of computational tools. They probably want to see the reference
documentation.

The fourth and final audience is potential engineering recruits. They come to
our website looking for four things: signs of life & dynamism, an interesting
technical explanation of Hail, information on the team (e.g. team culture), and
the source code.

---

All four users (unfortunately) land on the same front-page. I believe there are
seven possible next steps:

- installation directions
- get answers to a question (could be a forum, could be a FAQ, ideally a search
  bar for both and all the docs)
- cookbooks/examples of applying Hail to data (ideally copy-pasteable, uses
  public data)
- explanation of what kind of science Hail enables and how (probably includes
  concrete examples like gnomAD)
- reference documentation
- technical explanation of what Hail is (i.e. how do we go from python code to
  executing a query on a dataset in the cloud)
- the source code

We probably don't even want seven options on the main page. The more options on
the landing page the more overwhelming it is, but the fewer options on the
landing page the more clicks need to arrive at your destination.

A user flow is probably a DAG or maybe just a graph:

- installation directions
  - how to install on mac os
    - Try a simple query to verify installation worked! (goto: first-query)
  - how to install on linux
    - Try a simple query to verify installation worked! (goto: first-query)
  - how to install on GCP
    - Try a simple query to verify installation worked! (goto: first-query-on-dataproc)
  - how to install on a private Spark cluster
    - Try a simple query to verify installation worked! (goto: first-query)

- first-query: a simple query that verifies installation succeeded
  - got an error? ask a question on the forum
  - how do I run a GWAS?
  - get hail cheatsheets
  - read a one-page overview of Tables, MatrixTables, and BlockMatrices
  - learn how to use hail on the cloud (goto: first-query-on-dataproc)

- first-query-on-dataproc: submit a simple query using dataproc
  - got an error? ask a question on the forum
  - how do I connect to a notebook?
  - how do I run a GWAS?
  - get hail cheatsheets
  - read a one-page overview of Tables, MatrixTables, and BlockMatrices
  - read a one-page overview of scaling, clusters, & the cloud

---

- get answers to a question (could be a forum, could be a FAQ, ideally a search
  bar for both and all the docs)

For now, this should probably link directly to
discuss.hail.is/new-topic?category=Help%20%5B0.2%5D

