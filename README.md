This is the first part of the rewrite of GoGraph (a Graph database written in Golang) in Rust.
So far it covers the "loader" component, which takes an rdf file and loads the node elements into Dynamodb with edge data stored in MySQL for later processing by the "join" component.

