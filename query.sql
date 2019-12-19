select state, count(1) as count
from homicides
group by state