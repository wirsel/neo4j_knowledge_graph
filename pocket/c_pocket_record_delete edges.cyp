match(n:c_pocket_record)-[e:time_added]-(:c_pocket_timeadded)
delete e;
match(n:c_pocket_record)<-[e:is_url_in_pocket_record]->(:c_url)
delete e;
match(n:c_pocket_record)-[e:has_raw_tags]->(:c_pocket_rawtags)
delete e;
match(n:c_pocket_record)-[e:has_raw_tag]->(:c_pocket_tagraw)
delete e;
match(n:c_pocket_rawtags)-[e:contains_raw_tag]->(:c_pocket_tagraw)
delete e;
