<<<<<<< HEAD
Project 2 starter code
Copyright (C) George Porter, 2017, 2018.

## Overview

This is the starter code for the Java implementation of SurfStore.

## To build the protocol buffer IDL into auto-generated stubs:

$ mvn protobuf:compile protobuf:compile-custom

## To build the code:

$ mvn package

## To run the services:

$ target/surfstore/bin/runBlockServer
$ target/surfstore/bin/runMetadataStore

## To run the client

$ target/surfstore/bin/runClient

## To delete all programs and object files

$ mvn clean
=======
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
>>>>>>> 9e400e554965c421fa2eef4f96312a7b0008a084
