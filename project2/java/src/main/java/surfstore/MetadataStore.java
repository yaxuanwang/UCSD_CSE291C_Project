package surfstore;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.*;


public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;

    private ManagedChannel blockChannel;
    private BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    public MetadataStore(ConfigReader config) {
        this.config = config;
        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);
//        config.getLeaderNum();
	}

	private void start(int port, int numThreads, boolean leader,
                       ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub> metadataStubs) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(blockStub, leader, metadataStubs))
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);

        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }
        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }
        final MetadataStore server = new MetadataStore(config);
        boolean leader = false;
        ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub> metadataStubs = new ArrayList<>();
        if (config.getLeaderNum() == c_args.getInt("number")) {
            leader = true;
            for (int i=1; i<=config.getNumMetadataServers(); i++) {
                if (i != config.getLeaderNum()) {
                    ManagedChannel metadataChannel = ManagedChannelBuilder
                            .forAddress("127.0.0.1", config.getMetadataPort(i)).usePlaintext(true).build();
                    MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub
                            = MetadataStoreGrpc.newBlockingStub(metadataChannel);
                    metadataStubs.add(metadataStub);
                }
            }
        }
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"),
                leader, metadataStubs);
        server.blockUntilShutdown();
    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {
        protected Map<String, Integer> versionMap;
        protected Map<String, ArrayList<String>> blockHashMap;
        private boolean isCrashed = false;
        private boolean isLeader;
        private BlockStoreGrpc.BlockStoreBlockingStub blockStub;
        private ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub> metadataStubs;
        private ArrayList<Log> logEntries;  //index start from 0
        private int clusterNum;


        public MetadataStoreImpl(BlockStoreGrpc.BlockStoreBlockingStub blockStub,
                                 boolean isLeader,
                                 ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub> metadataStubs) {
            super();
            this.versionMap = new HashMap<>();
            this.blockHashMap = new HashMap();
            this.blockStub = blockStub;
            this.isLeader = isLeader;
            this.metadataStubs = metadataStubs;
            this.logEntries = new ArrayList<>();
            this.clusterNum = metadataStubs.size() + 1;

            if (this.clusterNum > 1 && this.isLeader) {
                Thread sync = new Thread() {
                    public void run() {
                        try {
                            while (true) {
                                syncLogs();
                                Thread.sleep(500);
                            }
                        } catch (InterruptedException v) {
                            System.out.println(v);
                        }
                    }
                };
                sync.start();
            }
        }


        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


        // TODO: Implement the other RPCs!
        @Override
        public void readFile(surfstore.SurfStoreBasic.FileInfo request,
                             io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
            //TODO: what to return if not leader??
            if (!isLeader) {
//                throw new RuntimeException("Calling non-leader server!");
                FileInfo.Builder builder = FileInfo.newBuilder();
                String filename = request.getFilename();
                if(!versionMap.containsKey(filename)) {
                    versionMap.put(filename, 0);
                }
                int version = versionMap.get(filename);
                builder.setFilename(filename);
                builder.setVersion(version);

                FileInfo response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }
            logger.info("Reading file: " + request.getFilename());

            FileInfo.Builder builder = FileInfo.newBuilder();
            String filename = request.getFilename();
            int version = 0;
            builder.setFilename(filename);
            if (versionMap.containsKey(filename)) {
                version = versionMap.get(filename);
            } else {
                logger.info("Warning: file " + request.getFilename() + " does not exist!");
                versionMap.put(filename, 0);
            }
            // TODO: Write log and append entries??
//            writeLog(version, "readFile", filename);
            builder.setVersion(version);

            if (blockHashMap.containsKey(filename)) {
                ArrayList<String> hashList = new ArrayList<>(blockHashMap.get(filename));
                builder.addAllBlocklist(hashList);
            }

            FileInfo response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


        private synchronized boolean syncLogs() {
            int vote = 1;
            if (logEntries.size()==0) return true;
            for (int i=0; i<metadataStubs.size(); i++) {
                int goal = logEntries.size()-1;
                AppendResult result = metadataStubs.get(i).appendEntries(logEntries.get(goal));
                if (result.getResult() == AppendResult.Result.OK) {
<<<<<<< HEAD
                    metadataStubs.get(i).commit(logEntries.get(goal));
=======
>>>>>>> 9e400e554965c421fa2eef4f96312a7b0008a084
                    vote++;
                } else if (result.getResult() == AppendResult.Result.MISSING_LOGS) {
                    //TODO: resend missing logs to sync
                    ArrayList<Integer> missingLogs = new ArrayList<>(result.getMissingLogsList());
                    for (Integer idx: missingLogs) {
                        metadataStubs.get(i).appendEntries(logEntries.get(idx));
                        metadataStubs.get(i).commit(logEntries.get(idx));
                        AppendResult check = metadataStubs.get(i).appendEntries(logEntries.get(goal));

                        // TODO: WHAT TO DO IF HAS BEEN SYNC
                        if (check.getResult() == AppendResult.Result.OK) {
                            vote++;
                            if(logEntries.get(goal).getIsCommited()) {
                                metadataStubs.get(i).commit(logEntries.get(goal));
                            }
                            break;
                        }
                    }
                }
            }
            return vote >= (clusterNum/2+1);
        }


        private synchronized void twoPhaseCommit(Log latestLog) {
            latestLog = latestLog.toBuilder().setIsCommited(true).build();
            logEntries.set(logEntries.size()-1, latestLog);
            // TODO: CHECK WHETHER THEY ARE THE SAME
            for(int i=0; i<metadataStubs.size(); i++) {
                metadataStubs.get(i).commit(latestLog);
            }
        }


        @Override
        public void modifyFile(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            logger.info("Modifying file: " + request.getFilename());
            WriteResult.Builder builder = WriteResult.newBuilder();

            if (!isLeader) {
                builder.setResult(WriteResult.Result.NOT_LEADER);
                WriteResult response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            String filename = request.getFilename();
            int version = request.getVersion();
            ArrayList<String> hashlist = new ArrayList<>(request.getBlocklistList());

            // create a new file or modify an existing file
            if (!versionMap.containsKey(filename) && version == 1
                    || versionMap.containsKey(filename) && version == versionMap.get(filename)+1) {
                boolean missing = false;
                ArrayList<String> missingBlocks = new ArrayList<>();
                for (String hash: hashlist) {
                    Block b = Block.newBuilder().setHash(hash).build();
                    if (!blockStub.hasBlock(b).getAnswer()) {
                        missing = true;
                        missingBlocks.add(hash);
                    }
                }
                if(missing) {
                    builder.addAllMissingBlocks(missingBlocks);
                    builder.setResult(WriteResult.Result.MISSING_BLOCKS);
                    builder.setCurrentVersion(version);
                } else {
                    // successfully modify files, two phase commit
                    if(blockHashMap.containsKey(filename) && equalHashList(blockHashMap.get(filename), hashlist)) {
                        builder.setResult(WriteResult.Result.OK);
                    } else {
                        writeLog(version, filename, hashlist);
                        if (syncLogs()) {
                            builder.setResult(WriteResult.Result.OK);
                            builder.setCurrentVersion(version);
                            versionMap.put(filename, version);
                            blockHashMap.put(filename, hashlist);
                            twoPhaseCommit(logEntries.get(logEntries.size()-1));
                        }
                    }
                }
                WriteResult response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else if (!versionMap.containsKey(filename)) {
                versionMap.put(filename, 0);
                builder.setResult(WriteResult.Result.OLD_VERSION);
                builder.setCurrentVersion(versionMap.get(filename));

                WriteResult response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
            //TODO: what if versionMap does not contain file while version!=1?
            else {
                builder.setResult(WriteResult.Result.OLD_VERSION);
                builder.setCurrentVersion(versionMap.get(filename));

                WriteResult response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        }

        private boolean equalHashList(ArrayList<String> list1, ArrayList<String> list2) {
            if(list1==null && list2==null)
                return true;
            if((list1 == null && list2 != null) || (list1 != null && list2 == null))
                return false;

            if(list1.size()!=list2.size())
                return false;
            for(String itemList1: list1)
            {
                if(!list2.contains(itemList1))
                    return false;
            }
            return true;
        }

        @Override
        public void deleteFile(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            logger.info("Deleting file: " + request.getFilename());
            WriteResult.Builder builder = WriteResult.newBuilder();

            if (!isLeader) {
                builder.setResult(WriteResult.Result.NOT_LEADER);
                WriteResult response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            String filename = request.getFilename();
            int version = request.getVersion();

            // file has already been deleted or does not exist
            if(!versionMap.containsKey(filename) || versionMap.get(filename)== 0) {
                logger.info("Warning: file " + filename + "does not exist!");
                versionMap.put(filename, 0);
                builder.setResult(WriteResult.Result.OLD_VERSION);
                builder.setCurrentVersion(versionMap.get(filename));

                WriteResult response = builder.build();
                responseObserver.onNext(response);
                return;
            }
            if (blockHashMap.containsKey(filename) && blockHashMap.get(filename).get(0).equals("0")) {
                logger.info("Warning: file " + filename + "does not exist!");
                builder.setResult(WriteResult.Result.OLD_VERSION);
                builder.setCurrentVersion(versionMap.get(filename));

                WriteResult response = builder.build();
                responseObserver.onNext(response);
                return;
            }

            if (versionMap.containsKey(filename) && version == versionMap.get(filename)+1) {
                ArrayList<String> deleteList = new ArrayList<>();
                deleteList.add("0");
                // successfully modify files, two phase commit
                writeLog(version, filename, deleteList);

                if (syncLogs()) {
                    builder.setResult(WriteResult.Result.OK);
                    builder.setCurrentVersion(version);
                    versionMap.put(filename, version);
                    blockHashMap.put(filename, deleteList);
                    twoPhaseCommit(logEntries.get(logEntries.size()-1));
                }

                WriteResult response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
            else if (versionMap.containsKey(filename) && version != versionMap.get(filename)+1){
                builder.setResult(WriteResult.Result.OLD_VERSION);
                builder.setCurrentVersion(versionMap.get(filename));

                WriteResult response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        }


        @Override
        public void isLeader(surfstore.SurfStoreBasic.Empty request,
                             io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            SimpleAnswer.Builder builder = SimpleAnswer.newBuilder();
            builder.setAnswer(isLeader);

            SimpleAnswer response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


        @Override
        public void crash(surfstore.SurfStoreBasic.Empty request,
                          io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            if (isLeader) {
                throw new RuntimeException("Leader will not crash!");
            }
            isCrashed = true;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


        @Override
        public void restore(surfstore.SurfStoreBasic.Empty request,
                            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            if (isLeader) {
                throw new RuntimeException("Leader doesn't need to restore!");
            }
            isCrashed = false;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


        @Override
        public void isCrashed(surfstore.SurfStoreBasic.Empty request,
                              io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            SimpleAnswer.Builder builder = SimpleAnswer.newBuilder();
            builder.setAnswer(isCrashed);

            SimpleAnswer response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


        @Override
        public void appendEntries(surfstore.SurfStoreBasic.Log request,
                                  io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.AppendResult> responseObserver) {
            AppendResult.Builder builder = AppendResult.newBuilder();
            if(isCrashed) {
//                throw new RuntimeException("Follower is crashed, cannot append!");
                builder.setResult(AppendResult.Result.IS_CRASHED);
                AppendResult response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }
            if(request.getIndex() > logEntries.size()) {
                builder.setResult(AppendResult.Result.MISSING_LOGS);
                int start = logEntries.size();
                int end = request.getIndex();
                ArrayList<Integer> missingIdx = new ArrayList<>();
                for (int i=start; i<end; i++) { //check index again
                    missingIdx.add(i);
                }
                builder.addAllMissingLogs(missingIdx);
            }  else if (request.getIndex() == logEntries.size()-1) {
                builder.setResult(AppendResult.Result.OK);
            } else {
                builder.setResult(AppendResult.Result.OK);
                logEntries.add(request);
            }

            AppendResult response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


        @Override
        public void commit(surfstore.SurfStoreBasic.Log request,
                           io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            if(isLeader) {
                throw new RuntimeException("Not a follower or follower is crashed, cannot commit!");
            }
            if(isCrashed) {
                Empty response = Empty.newBuilder().build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }
            int size = logEntries.size();
            int curIdx = logEntries.get(size-1).getIndex();
            if(request.getIndex() != curIdx) {
                throw new RuntimeException("Commit Error!");
            }
            if(logEntries.get(curIdx).getIsCommited() || request.getIsCommited()) {
                Log commitLog = logEntries.get(curIdx).toBuilder().setIsCommited(true).build();
                logEntries.set(curIdx, commitLog);
                versionMap.put(request.getFilename(), request.getVersion());
                ArrayList<String> hashlist = new ArrayList<>(request.getHashlistList());
                blockHashMap.put(request.getFilename(), hashlist);
            }

            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


        @Override
        public void getVersion(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
            FileInfo.Builder builder = FileInfo.newBuilder();
            String filename = request.getFilename();
            ArrayList<Integer> versionList = new ArrayList<>();
            builder.setFilename(filename);

//            if(clusterNum == 1) {
                if (!versionMap.containsKey(filename)) {
                    versionMap.put(filename, 0);
                }
                int version = versionMap.get(filename);
                builder.setVersion(version);
//            } else {
//                if (isLeader) {
//                    if (versionMap.containsKey(filename)) {
//                        int version = versionMap.get(filename);
//                        versionList.add(version);
//                        builder.setVersion(version);
//                    } else {
//                        versionMap.put(filename, 0);
//                        versionList.add(0);
//                        builder.setVersion(0);
//                    }
//                    for (int i = 0; i < metadataStubs.size(); i++) {
//                        FileInfo info = FileInfo.newBuilder().setFilename(filename).build();
//                        int version = metadataStubs.get(i).getVersion(info).getVersion();
//                        versionList.add(version);
//                    }
//                    builder.addAllVersionlist(versionList);
//                } else {
//                    if (versionMap.containsKey(filename)) {
//                        int version = versionMap.get(filename);
//                        builder.setVersion(version);
//                    } else {
//                        versionMap.put(filename, 0);
//                        builder.setVersion(0);
//                    }
//                }
//            }

            FileInfo response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


//        @Override
//        public void getVersion(surfstore.SurfStoreBasic.FileInfo request,
//                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
//            FileInfo.Builder builder = FileInfo.newBuilder();
//            String filename = request.getFilename();
//            ArrayList<Integer> versionList = new ArrayList<>();
//            builder.setFilename(filename);
//            if (versionMap.containsKey(filename)) {
//                builder.setVersion(versionMap.get(filename));
//            } else {
//                versionMap.put(filename, 0);
//                builder.setVersion(versionMap.get(filename));
//            }
//
//            FileInfo response = builder.build();
//            responseObserver.onNext(response);
//            responseObserver.onCompleted();
//        }


        private void writeLog (int version, String filename, ArrayList<String> hashlist) {
            Log.Builder builder = Log.newBuilder();
            builder.setVersion(version);
            builder.setIsCommited(false);
            builder.setFilename(filename);
            builder.addAllHashlist(hashlist);
            builder.setIndex(logEntries.size());

            logEntries.add(builder.build());
        }

    }
}