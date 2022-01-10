# Helper script for testing - Not needed for any production work

type="dollargeneral-popshelf"
dt=20211028
schedule="Weekly"
# grep_cond="transactions|tenders|transaction_item|trans_disc_xref|product|product_category|organization|discounts'"
grep_cond="transactions|tenders|transaction_item|trans_disc_xref|discounts"
# grep_cond="product|product_category|organization"
for OUTPUT in $(aws s3 ls s3://bridg-client-ftp/$type/1010/$schedule --recursive | awk ' {print $4}' | grep $dt | grep -E $grep_cond)
	do
	 file=$(echo " s3://bridg-client-ftp/$OUTPUT")
	 folder=$(echo "s3://bridg-client-ftp/$OUTPUT" | cut -d'/' -f7)
	 mkdir $folder
	 echo "aws s3 cp $file $folder; aws s3 cp --profile dev $folder/ s3://dev-cdp-data-lake/$type/1010/$schedule/$folder/ --recursive"
	 aws s3 cp $file $folder;
	 aws s3 cp --profile dev $folder/ s3://dev-cdp-data-lake/$type/1010/$schedule/$folder/ --recursive;
	done


type="dollargeneral-popshelf"
dt=20211028
schedule="Daily"
# grep_cond="transactions|tenders|transaction_item|trans_disc_xref|product|product_category|organization|discounts'"
grep_cond="transactions|tenders|transaction_item|trans_disc_xref|discounts"
# grep_cond="product|product_category|organization"
for OUTPUT in $(aws s3 ls s3://bridg-client-ftp/$type/1010/$schedule --recursive | awk ' {print $4}' | grep $dt | grep -E $grep_cond)
	do
	 file=$(echo " s3://bridg-client-ftp/$OUTPUT")
	 folder=$(echo "s3://bridg-client-ftp/$OUTPUT" | cut -d'/' -f7)
	 mkdir -p $folder
	 echo "aws s3 cp $file $folder; aws s3 cp --profile dev $folder/ s3://dev-cdp-data-lake/$type/1010/$schedule/$folder/ --recursive"
	 aws s3 cp $file $folder;
	 aws s3 cp --profile dev $folder/ s3://dev-cdp-data-lake/$type/1010/$schedule/$folder/ --recursive;
	done


type="dollargeneral-popshelf"
dt=20211028
schedule="Daily"
# grep_cond="transactions|tenders|transaction_item|trans_disc_xref|product|product_category|organization|discounts'"
# grep_cond="transactions|tenders|transaction_item|trans_disc_xref|discounts"
grep_cond="product|product_category|organization"
for OUTPUT in $(aws s3 ls s3://bridg-client-ftp/$type/1010/$schedule --recursive | awk ' {print $4}' | grep $dt | grep -E $grep_cond)
	do
	 file=$(echo " s3://bridg-client-ftp/$OUTPUT")
	 folder=$(echo "s3://bridg-client-ftp/$OUTPUT" | cut -d'/' -f7)
	 mkdir -p $folder
	 echo "aws s3 cp $file $folder; aws s3 cp --profile dev $folder/ s3://dev-cdp-data-lake/$type/1010/$schedule/$folder/ --recursive"
	 aws s3 cp $file $folder;
	 aws s3 cp --profile dev $folder/ s3://dev-cdp-data-lake/$type/1010/$schedule/$folder/ --recursive;
	done
