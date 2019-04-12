//S_gsheet labels
CREATE CONSTRAINT ON (n:S_gsheet) ASSERT n.identifier IS UNIQUE;
CREATE CONSTRAINT ON (n:C_gsheet_file) ASSERT n.identifier IS UNIQUE;
CREATE CONSTRAINT ON (n:C_gsheet_row) ASSERT n.identifier IS UNIQUE;
CREATE CONSTRAINT ON (n:C_gsheet_pocket_entry) ASSERT n.identifier IS UNIQUE;
