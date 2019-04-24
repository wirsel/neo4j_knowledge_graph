with $lg as lg //a C_log node expected as input
with *, "<<url>>"as urlorig
with replace(replace(urlorig,"://github.com","://raw.githubusercontent.com"),"/blob","") as urlraw

set lg.<<resKey>> = urlraw
rerurn null
