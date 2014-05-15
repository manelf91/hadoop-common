#!/usr/bin/env python
import subprocess as sp
import re
import os
import sys

job = sys.argv[1]

home = sp.check_output("wget -q  --output-document - 'http://localhost:50030/JobView.jsp'", shell=True)

links = []
links.append(re.findall(r'(?<=href=\")[^\"]*(?=\")', home))

links = links[0]
for link in links:
	if job in link:
		page_link='localhost:50030/' + link
		os.system("wget -qmk " + page_link)
		os.rename("localhost:50030", job)
		
