#!/bin/bash

# Copyright (c) 2020 Bridg Inc. All rights reserved.
# @author Sohil Jain <sohil.jain@bridg.com>

#spark-submit --name "My app" --master spark://10.200.10.88:7077 --py-files src/main/pgp_decrypt_upload.py

#remote_host_dir='hadoop@10.200.10.88:/home/hadoop/dg_transformer'
remote_host_dir='ubuntu@10.200.22.134:~/Projects/dg_transformer_history/'
#scp -r -i ~/.ssh/cdp.pem ~/PycharmProjects/dg_transformer/src/main ${remote_host_dir}/src/main
#scp -r -i ~/.ssh/cdp.pem ~/PycharmProjects/dg_transformer/src/main/transformer.py ${remote_host_dir}/src/main/transformer.py
#scp -r -i ~/.ssh/cdp.pem ~/PycharmProjects/dg_transformer/start_transformer.sh ${remote_host_dir}start_transformer.sh
#scp -r -i ~/.ssh/cdp.pem ~/PycharmProjects/dg_transformer/Pipfile ${remote_host_dir}Pipfile
#scp -r -i ~/.ssh/cdp.pem ~/PycharmProjects/dg_transformer/Pipfile.lock ${remote_host_dir}Pipfile.lock
scp -i ~/.ssh/cdp.pem ~/PycharmProjects/bridg-dollargeneral-transformer/Dockerfile ${remote_host_dir}Dockerfile
scp -i ~/.ssh/cdp.pem ~/PycharmProjects/bridg-dollargeneral-transformer/Jenkinsfile ${remote_host_dir}Jenkinsfile

scp -i ~/.ssh/cdp.pem ~/PycharmProjects/bridg-dollargeneral-transformer/src/main/pgp_decrypt_upload_daily.py ${remote_host_dir}/src/main/pgp_decrypt_upload_daily.py
scp -i ~/.ssh/cdp.pem ~/PycharmProjects/bridg-dollargeneral-transformer/src/main/pgp_decrypt_upload_historical.py ${remote_host_dir}/src/main/pgp_decrypt_upload_historical.py
scp -i ~/.ssh/cdp.pem ~/PycharmProjects/bridg-dollargeneral-transformer/src/main/emr_process_gold_historical.py ${remote_host_dir}/src/main/emr_process_gold_historical.py
scp -i ~/.ssh/cdp.pem ~/PycharmProjects/bridg-dollargeneral-transformer/src/main/emr_process_gold_daily.py ${remote_host_dir}/src/main/emr_process_gold_daily.py
scp -i ~/.ssh/cdp.pem ~/PycharmProjects/bridg-dollargeneral-transformer/src/main/run_job_flow.py ${remote_host_dir}/src/main/run_job_flow.py
scp -i ~/.ssh/cdp.pem ~/PycharmProjects/bridg-dollargeneral-transformer/src/main/run_job_flow_historical.py ${remote_host_dir}/src/main/run_job_flow_historical.py
scp -i ~/.ssh/cdp.pem ~/PycharmProjects/bridg-dollargeneral-transformer/src/run.sh ${remote_host_dir}/src/run.sh
#
#
ssh -i ~/.ssh/cdp.pem ubuntu@10.200.22.134 -t "
cd /home/ubuntu/Projects/dg_transformer_history && ./start_transformer.sh
"

#52 22 * * * (cd /home/ubuntu/Projects/dg_transformer && ./start_transformer.sh)  >> /bridg_logs/transformer/bridg_$($DATEVAR).log 2>&1
#scp -r -i ~/.ssh/cdp.pem ~/PycharmProjects/dg_transformer/ hadoop@10.200.10.88:/home/hadoop/dg_transformer/
