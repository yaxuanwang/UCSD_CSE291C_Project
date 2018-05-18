package surfstore;

import java.io.File;
import java.io.IOException;
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
            for (int i=1; i<config.getNumMetadataServers(); i++) {
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
        private ArrayList<Log> logEntries;
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
                throw new RuntimeException("Calling non-leader server!");
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
            // Write log and append entries
            writeLog(version, "readFile", filename);
            int vote = 0;
            for (int i=0; i<metadataStubs.size(); i++) {
                Empty crash = Empty.newBuilder().build();
                if (!metadataStubs.get(i).isCrashed(crash).getAnswer()) {
                    AppendResult result = metadataStubs.get(i).appendEntries(logEntries.get(logEntries.size() - 1));
                    if (result.getResult() == AppendResult.Result.OK) {
                        vote++;
                    } else if (result.getResult() == AppendResult.Result.MISSING_LOGS) {
                        //TODO: resend missing logs to sync
                    }
                }
            }
            builder.setVersion(version);

            if (blockHashMap.containsKey(filename)) {
                ArrayList<String> hashList = new ArrayList<>(blockHashMap.get(filename));
                builder.addAllBlocklist(hashList);
            }

            FileInfo response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
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
                int i = 0;
                ArrayList<String> missingBlocks = new ArrayList<>();
                for (String hash: hashlist) {
                    Block b = Block.newBuilder().setHash(hash).build();
                    if (!blockStub.hasBlock(b).getAnswer()) {
                        missing = true;
                        missingBlocks.add(hash);
                        i++;
                    }
                }
                if(missing) {
                    builder.addAllMissingBlocks(missingBlocks);
                    builder.setResult(WriteResult.Result.MISSING_BLOCKS);
                    builder.setCurrentVersion(version);
                } else {
                    builder.setResult(WriteResult.Result.OK);
                    builder.setCurrentVersion(version);
                    versionMap.put(filename, version);
                    blockHashMap.put(filename, hashlist);
                }
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

            // file has already been deleted
            if (blockHashMap.containsKey(filename)
                    && blockHashMap.get(filename).get(0) == "0") {
                //TODO: what to do?
                logger.info("Warning: file " + filename + " has been deleted!");
            }
            if (versionMap.containsKey(filename) && version == versionMap.get(filename)+1) {
                builder.setResult(WriteResult.Result.OK);
                builder.setCurrentVersion(version);
                versionMap.put(filename, version);
                ArrayList<String> deleteList = new ArrayList<>();
                deleteList.add("0");
                blockHashMap.put(filename, deleteList);

                WriteResult response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
            //TODO: what if versionMap does not contain file while version!=1?
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

        }


        private void writeLog (int version, String operation, String filename) {
            Log.Builder builder = Log.newBuilder();
            builder.setVersion(version);
            builder.setOperation(operation);
            builder.setFilename(filename);

            logEntries.add(builder.build());
        }

    }
}