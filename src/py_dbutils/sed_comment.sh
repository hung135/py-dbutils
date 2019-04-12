#!/bin/bash
sed 's/^.*\(def\)\( .*\)\((.*\)/&\
        """Describe Method:\
\
        Args:\3\
          table_name (str): String\
\
        Returns:\
          None: None\
        """/g' $1 >$2
