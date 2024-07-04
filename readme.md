Files that we need:

Steps:
1. extract_text_parallel.py
2. split_pdfs.py


TO DO:
1. make sure that we modify split_pdfs to add the map to the correct one


Run script and keep last 1000 lines:
nohup python3 extract_fields.py & tail -n 1000 -f nohup.out > nohup_temp.out && mv nohup_temp.out nohup.out &

and then run
while true; do
    tail -n 1000 nohup.out > nohup_temp.out
    mv nohup_temp.out nohup.out
    sleep 60  # Adjust the sleep duration as needed
done &