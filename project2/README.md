 ## Overview
 
 This is the starter code for the Java implementation of SurfStore.

 $ target/surfstore/bin/runBlockServer
 $ target/surfstore/bin/runMetadataStore
 
 For Distributed scenario: 
 
 $ target/surfstore/bin/runMetadataStore ../configs/configDistributed.txt -n No.MSD
 
 ## To run the client
 
 $ target/surfstore/bin/runClient
 
 Client now supports following commands:
 
 $ client /etc/myconfig.txt getversion myfile.txt
 
 $ client /etc/myconfig.txt delete myfile.txt
 
 $ client /etc/myconfig.txt upload /home/aturing/myfile.txt
 
 $ client /etc/myconfig.txt download myfile.txt /home/aturing/downloads
 
 $ client /etc/myconfig.txt crash No.MSD
 
 $ client /etc/myconfig.txt restore No.MSD
 
 $ client /etc/myconfig.txt isCrash No.MSD
 
 
 ## To delete all programs and object files
 
 $ mvn clean
