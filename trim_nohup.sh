   #!/bin/bash
   while true; do
       tail -n 1000 nohup.out > nohup_temp.out
       mv nohup_temp.out nohup.out
       sleep 60  # Adjust the sleep duration as needed
   done