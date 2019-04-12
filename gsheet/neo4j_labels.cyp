//S_gsheet labels
CREATE CONSTRAINT ON (n:S_gsheet) ASSERT n.identifier IS UNIQUE;
CREATE CONSTRAINT ON (n:C_gsheet_file) ASSERT n.identifier IS UNIQUE;
CREATE CONSTRAINT ON (n:C_gsheet_row) ASSERT n.identifier IS UNIQUE;
CREATE CONSTRAINT ON (n:C_gsheet_pocket_entry) ASSERT n.identifier IS UNIQUE;
match(p:C_gsheet_file) set p:S_gsheet return count(p);
match(p:C_gsheet_row) set p:S_gsheet return count(p);
match(p:C_gsheet_pocket_entry) set p:S_gsheet return count(p);
