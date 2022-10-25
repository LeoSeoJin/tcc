#include "kvservice/kv_server.h"

namespace scc {

    KVTxServer::KVTxServer(std::string name, unsigned short publicPort, int totalNumKeys) {
        _serverName = name;
        _publicPort = publicPort;
        _coordinator = new CoordinatorTx(_serverName, publicPort, totalNumKeys);
    }

    KVTxServer::KVTxServer(std::string name,
                           unsigned short publicPort,
                           unsigned short partitionPort,
                           unsigned short replicationPort,
                           int partitionId,
                           int replicaId,
                           int totalNumKeys,
                           std::string groupServerName,
                           int groupServerPort) {
        _serverName = name;
        _publicPort = publicPort;
        _partitionPort = partitionPort;
        _replicationPort = replicationPort;
        _partitionId = partitionId;
        _replicaId = replicaId;
        _coordinator = new CoordinatorTx(_serverName,
                                         _publicPort,
                                         _partitionPort,
                                         _replicationPort,
                                         partitionId,
                                         replicaId,
                                         totalNumKeys,
                                         groupServerName,
                                         groupServerPort);
    }

    KVTxServer::~KVTxServer() {
        delete _coordinator;
    }

    void KVTxServer::Run() {
        // partitions of the same replication group
        std::thread tPartition(&KVTxServer::ServePartitionConnection, this);
        tPartition.detach();

        std::thread tPartitionW(&KVTxServer::ServePartitionsWriteConnection, this);
        tPartitionW.detach();

        // replicas of the same partition
        std::thread tReplication(&KVTxServer::ServeReplicationConnection, this);
        tReplication.detach();

        // initialize
        _coordinator->Initialize();

        // serve operation request from clients
        ServePublicConnection();
    }

    void KVTxServer::ServePublicConnection() {
        try {
            TCPServerSocket serverSocket(_publicPort);
            for (;;) {
                TCPSocket *clientSocket = serverSocket.accept();
                std::thread t(&KVTxServer::HandlePublicRequest, this, clientSocket);
                t.detach();
            }
        } catch (SocketException &e) {
            fprintf(stdout, "Server public socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "[SEVERE FAILURE ServePublicConnection] Server public socket exception: %s\n", "...");
            fflush(stdout);
        }
    }

    void KVTxServer::HandlePublicRequest(TCPSocket *clientSocket) {
        try {
            RPCServerPtr rpcServer(new RPCServer(clientSocket));
            for (;;) {

                // get rpc request
                PbRpcRequest rpcRequest;
                PbRpcReply rpcReply;
                rpcServer->RecvRequest(rpcRequest);

                switch (static_cast<RPCMethod> (rpcRequest.methodid())) {
                    ////////////////////////////////
                    // key-value store interfaces //
                    ////////////////////////////////

                    case RPCMethod::GetServerConfig: {
                        PbRpcTxPublicGetServerConfigResult opResult;
                        opResult.set_succeeded(true);

                        opResult.set_numpartitions(_coordinator->NumPartitions());
                        opResult.set_numreplicasperpartition(_coordinator->NumReplicasPerPartition());
                        opResult.set_replicaid(_coordinator->ReplicaId());

                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);
                        break;
                    }

                    case RPCMethod::EchoTest: {
                        // get arguments
                        PbRpcEchoTest echoRequest;
                        PbRpcEchoTest echoReply;
                        echoRequest.ParseFromString(rpcRequest.arguments());
                        echoReply.set_text(echoRequest.text());
                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(echoReply.SerializeAsString());
                        rpcServer->SendReply(rpcReply);
                        break;
                    }
					
                    case RPCMethod::TxStart: {

#ifdef H_CURE
                        PbRpcTccWrenPublicStartArg opArg;
                        PbRpcTccWrenPublicStartResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleTxStart<PbRpcTccWrenPublicStartArg, PbRpcTccWrenPublicStartResult>(opArg, opResult);
#endif

#if defined(WREN)
                        PbRpcTccNBWrenPublicStartArg opArg;
                        PbRpcTccNBWrenPublicStartResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleTxStart<PbRpcTccNBWrenPublicStartArg, PbRpcTccNBWrenPublicStartResult>(opArg, opResult);

#endif

#if defined(CURE)
                        PbRpcTccCurePublicStartArg opArg;
                        PbRpcTccCurePublicStartResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleTxStart<PbRpcTccCurePublicStartArg, PbRpcTccCurePublicStartResult>(opArg, opResult);

#endif
                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);


#ifdef MEASURE_STATISTICS
                        SysStats::NumPublicTxStartRequests++;
#endif

                        break;
                    }

#if defined(CURE)
                    case RPCMethod::MyStart: {

                        PbRpcCurePublicStartArg opArg;
                        PbRpcCurePublicStartResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleMyStart<PbRpcCurePublicStartArg, PbRpcCurePublicStartResult>(opArg, opResult);

                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);


#ifdef MEASURE_STATISTICS
						SysStats::NumPublicStartRequests++;
#endif

                        break;
                    }

                    case RPCMethod::MyGet: {


                        PbRpcCureKVPublicReadArg opArg;
                        PbRpcCureKVPublicReadResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleMyGet(opArg, opResult);

                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);

#ifdef MEASURE_STATISTICS
                        SysStats::NumPublicGetRequests++;
#endif
                        break;
                    }

					case RPCMethod::MyPut: {
        				PbRpcCureKVPublicWriteArg opArg;
				        PbRpcCureKVPublicWriteResult opResult;		
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleMyPut(opArg, opResult);

                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);

#ifdef MEASURE_STATISTICS
                        SysStats::NumPublicPutRequests++;
#endif				
						break;
					}
#endif //CURE

#ifdef TUNABLE_CAUSAL
                    case RPCMethod::TcGet: {

                        PbRpcTcCureKVPublicReadArg opArg;
                        PbRpcTcCureKVPublicReadResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleTcGet<PbRpcTcCureKVPublicReadArg, PbRpcTcCureKVPublicReadResult>(opArg, opResult);

                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);

#ifdef MEASURE_STATISTICS
                        SysStats::NumPublicGetRequests++;
#endif
                        break;
                    }

					case RPCMethod::TcPut: {
        				PbRpcTcCureKVPublicWriteArg opArg;
				        PbRpcTcCureKVPublicWriteResult opResult;		
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleTcPut(opArg, opResult);

                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);

#ifdef MEASURE_STATISTICS
                        SysStats::NumPublicPutRequests++;
#endif				
						break;
					}
#endif //TUNABLE_CAUSAL

                    case RPCMethod::TxRead: {

#ifdef H_CURE
                        PbRpcTccWrenKVPublicReadArg opArg;
                        PbRpcTccWrenKVPublicReadResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleTxRead<PbRpcTccWrenKVPublicReadArg, PbRpcTccWrenKVPublicReadResult>(opArg, opResult);

#elif defined(WREN)
                        PbRpcTccNBWrenKVPublicReadArg opArg;
                        PbRpcTccNBWrenKVPublicReadResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleTxRead<PbRpcTccNBWrenKVPublicReadArg, PbRpcTccNBWrenKVPublicReadResult>(opArg, opResult);
#elif defined(CURE)
                        PbRpcTccCureKVPublicReadArg opArg;
                        PbRpcTccCureKVPublicReadResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleTxRead<PbRpcTccCureKVPublicReadArg, PbRpcTccCureKVPublicReadResult>(opArg, opResult);
#endif
                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);

#ifdef MEASURE_STATISTICS
                        SysStats::NumPublicTxReadRequests++;
#endif
                        break;
                    }

                    case RPCMethod::TxCommit: {
#ifdef H_CURE
                        PbRpcKVPublicCommitArg opArg;
                        PbRpcKVPublicCommitResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleTxCommit(opArg, opResult);

#elif defined(WREN)
                        PbRpcTccNBWrenKVPublicCommitArg opArg;
                        PbRpcTccNBWrenKVPublicCommitResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleTxCommit(opArg, opResult);

#elif defined(CURE)
                        PbRpcTccCureKVPublicCommitArg opArg;
                        PbRpcTccCureKVPublicCommitResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());


                        HandleTxCommit(opArg, opResult);
#endif

                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);

#ifdef MEASURE_STATISTICS
                        SysStats::NumPublicTxCommitRequests++;
#endif

                        break;
                    }

                    case RPCMethod::ShowItem: {
                        // get arguments
                        PbRpcKVPublicShowArg opArg;
                        PbRpcKVPublicShowResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());
                        // execute op
                        std::string itemVersions;
                        bool ret = _coordinator->ShowItem(opArg.key(), itemVersions);
                        opResult.set_succeeded(ret);
                        if (ret) {
                            opResult.set_returnstring(itemVersions);
                        }
                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);
                        break;
                    }

                    case RPCMethod::ShowDB: {
                        // get arguments
                        PbRpcKVPublicShowResult opResult;
                        // execute op
                        std::string allItemVersions;
                        bool ret = _coordinator->ShowDB(allItemVersions);
                        opResult.set_succeeded(ret);
                        if (ret) {
                            opResult.set_returnstring(allItemVersions);
                        }
                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);
                        break;
                    }

                    case RPCMethod::ShowState: {
                        // get arguments
                        PbRpcKVPublicShowResult opResult;
                        // execute op
                        std::string stateStr = (boost::format("*%s|") % _serverName).str();
                        bool ret = _coordinator->ShowState(stateStr);
                        opResult.set_succeeded(ret);
                        if (ret) {

                            opResult.set_returnstring(stateStr);
                        }
                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);

                        break;
                    }

                    case RPCMethod::ShowStateCSV: {
                        // get arguments
                        PbRpcKVPublicShowResult opResult;
                        // execute op
                        std::string stateStr = (boost::format("%s") % _serverName).str();
                        bool ret = _coordinator->ShowStateCSV(stateStr);
                        opResult.set_succeeded(ret);
                        if (ret) {

                            opResult.set_returnstring(stateStr);
                        }
                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);

                        break;
                    }

                    case RPCMethod::DumpLatencyMeasurement: {
                        // get arguments
                        PbRpcKVPublicShowResult opResult;
                        // execute op
                        std::string resultStr;
                        bool ret = _coordinator->DumpLatencyMeasurement(resultStr);
                        opResult.set_succeeded(ret);
                        if (ret) {
                            opResult.set_returnstring(resultStr);
                        }
                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);
                        break;
                    }

                    default:
                        throw KVOperationException("(public) Unsupported operation.");
                }
            }
        } catch (SocketException &e) {

            fprintf(stdout, "HandlePublicRequest:Client serving thread socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "SEVERE Server public socket exception: %s\n", "...");
            fflush(stdout);
        }
    }

    template<class Argument, class Result>
    void KVTxServer::HandleTxStart(Argument &opArg, Result &opResult) {
        TxConsistencyMetadata cdata;

        //Process the received arguments
#ifdef H_CURE
        const PbPhysicalTimeSpec &v = opArg.ldt();
        cdata.LDT.Seconds = v.seconds();
        cdata.LDT.NanoSeconds = v.nanoseconds();

        const PbPhysicalTimeSpec &v2 = opArg.rst();
        cdata.RST.Seconds = v2.seconds();
        cdata.RST.NanoSeconds = v2.nanoseconds();

#elif defined(WREN)
        const PbPhysicalTimeSpec &v = opArg.glst();
        cdata.GLST.Seconds = v.seconds();
        cdata.GLST.NanoSeconds = v.nanoseconds();

        const PbPhysicalTimeSpec &v2 = opArg.grst();
        cdata.GRST.Seconds = v2.seconds();
        cdata.GRST.NanoSeconds = v2.nanoseconds();

#elif defined(CURE)
        int i;
        const int numReplicas = _coordinator->NumReplicasPerPartition();
        for (i = 0; i < numReplicas; i++) {
            const PbPhysicalTimeSpec &v = opArg.dv(i);
            PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
            cdata.DV.push_back(p);
        }
#endif

        bool ret = _coordinator->TxStart(cdata);
        opResult.set_succeeded(ret);

        if (ret) {
            opResult.set_id(cdata.txId);

#ifdef H_CURE
            opResult.mutable_rst()->set_seconds(cdata.RST.Seconds);
            opResult.mutable_rst()->set_nanoseconds(cdata.RST.NanoSeconds);

#elif defined(WREN)
            opResult.mutable_grst()->set_seconds(cdata.GRST.Seconds);
            opResult.mutable_grst()->set_nanoseconds(cdata.GRST.NanoSeconds);

            opResult.mutable_glst()->set_seconds(cdata.GLST.Seconds);
            opResult.mutable_glst()->set_nanoseconds(cdata.GLST.NanoSeconds);

#elif defined(CURE)
            assert(numReplicas == cdata.DV.size());
            for (i = 0; i < numReplicas; i++) {
                PbPhysicalTimeSpec *p1 = opResult.add_dv();
                p1->set_seconds(cdata.DV[i].Seconds);
                p1->set_nanoseconds(cdata.DV[i].NanoSeconds);
            }

#endif
        }
    }

    template<class Argument, class Result>
    void KVTxServer::HandleMyStart(Argument &opArg, Result &opResult) {
        TxConsistencyMetadata cdata;

        //Process the received arguments
        int i;
        const int numReplicas = _coordinator->NumReplicasPerPartition();
        for (i = 0; i < numReplicas; i++) {
            const PbPhysicalTimeSpec &v = opArg.dv(i);
            PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
            cdata.DV.push_back(p);
        }

        bool ret = _coordinator->MyStart(cdata);
        opResult.set_succeeded(ret);

        if (ret) {
            opResult.set_id(cdata.txId);
            assert(numReplicas == cdata.DV.size());
            for (i = 0; i < numReplicas; i++) {
                PbPhysicalTimeSpec *p1 = opResult.add_dv();
                p1->set_seconds(cdata.DV[i].Seconds);
                p1->set_nanoseconds(cdata.DV[i].NanoSeconds);
            }
        }
    }

#ifdef CURE
    void KVTxServer::HandleMyGet(PbRpcCureKVPublicReadArg &opArg, PbRpcCureKVPublicReadResult &opResult) {
        TxConsistencyMetadata cdata;
        std::vector<std::string> keySet;
        std::vector<std::string> valueSet;

        for (int i = 0; i < opArg.key_size(); ++i) {
            keySet.push_back(opArg.key(i));
        }

        cdata.txId = opArg.id();

        bool ret = _coordinator->MyGet(cdata.txId, keySet, valueSet);

        opResult.set_succeeded(ret);

        if (ret) {
            for (unsigned int i = 0; i < valueSet.size(); ++i) {
                opResult.add_value(valueSet[i]);
            }

			const int numReplicas = _coordinator->NumReplicasPerPartition();
        	Transaction *tx = _coordinator->GetTransaction(cdata.txId);	
			TxContex *data = tx->txContex;		
			assert(numReplicas == tx->txContex->DV.size());

            for (unsigned int i = 0; i < numReplicas; ++i) {
                PbPhysicalTimeSpec *pt = opResult.add_ut();
                pt->set_seconds(data->DV[i].Seconds);
                pt->set_nanoseconds(data->DV[i].NanoSeconds);
            }
        }
    }
#endif

#ifdef TUNABLE_CAUSAL

    template<class Argument, class Result>
    void KVTxServer::HandleTcGet(Argument &opArg, Result &opResult) {
        TxConsistencyMetadata cdata;
        std::vector<std::string> keySet;
        std::vector<std::string> valueSet;
		int level;

        for (int i = 0; i < opArg.key_size(); ++i) {
            keySet.push_back(opArg.key(i));
        }

        cdata.txId = opArg.id();
		level = opArg.level();
		
		//SLOG((boost::format("[Level]: KVserver.HandleTcGet, level %d.") % level).str());

        bool ret = _coordinator->TcGet(cdata.txId, keySet, valueSet, level);
        opResult.set_succeeded(ret);

        if (ret) {
            for (unsigned int i = 0; i < valueSet.size(); ++i) {
                opResult.add_value(valueSet[i]);
            }

			const int numReplicas = _coordinator->NumReplicasPerPartition();
        	Transaction *tx = _coordinator->GetTransaction(cdata.txId);	
			TxContex *data = tx->txContex;		
			assert(numReplicas == tx->txContex->DV.size());
			//std::vector<PhysicalTimeSpec> gsv = _coordinator->getGsv();

            for (unsigned int i = 0; i < numReplicas; ++i) {
                PbPhysicalTimeSpec *pt = opResult.add_ut();
                pt->set_seconds(data->RDV[i].Seconds);
                pt->set_nanoseconds(data->RDV[i].NanoSeconds);

                PbPhysicalTimeSpec *css = opResult.add_css();
                css->set_seconds(data->HRVC[i].Seconds);
                css->set_nanoseconds(data->HRVC[i].NanoSeconds);				
            }
        }
    }
#endif

#if defined(CURE) || defined(H_CURE) || defined(WREN)

    template<class Argument, class Result>
    void KVTxServer::HandleTxRead(Argument &opArg, Result &opResult) {
        TxConsistencyMetadata cdata;
        std::vector<std::string> keySet;
        std::vector<std::string> valueSet;

        for (int i = 0; i < opArg.key_size(); ++i) {
            keySet.push_back(opArg.key(i));
        }

        cdata.txId = opArg.id();

        bool ret = _coordinator->TxRead(cdata.txId, keySet, valueSet);

        opResult.set_succeeded(ret);

        if (ret) {
            for (unsigned int i = 0; i < valueSet.size(); ++i) {
                opResult.add_value(valueSet[i]);
            }

        }

    }

#endif

#ifdef H_CURE

    void KVTxServer::HandleTxCommit(PbRpcKVPublicCommitArg &opArg, PbRpcKVPublicCommitResult &opResult) {
        TxConsistencyMetadata cdata;
        std::vector<std::string> keySet;
        std::vector<std::string> valueSet;

        cdata.txId = opArg.id();

        /* A read-only transaction still needs to be committed in order to clean-up */
        if (opArg.key_size() != 0) {

            for (int i = 0; i < opArg.key_size(); ++i) {
                keySet.push_back(opArg.key(i));
                valueSet.push_back(opArg.value(i));
            }
        }


        bool ret = _coordinator->TxCommit(cdata, keySet, valueSet);


        opResult.set_succeeded(ret);
        if (ret) {
#ifdef DEBUG_MSGS
            SLOG((boost::format("TXID : %d committed with commitTime = %s") % cdata.txId %
                  Utils::physicaltime2str(cdata.CT).c_str()).str());
#endif
            opResult.mutable_ct()->set_seconds(cdata.CT.Seconds);
            opResult.mutable_ct()->set_nanoseconds(cdata.CT.NanoSeconds);
        }

    }

#endif

#ifdef WREN

    void
    KVTxServer::HandleTxCommit(PbRpcTccNBWrenKVPublicCommitArg &opArg, PbRpcTccNBWrenKVPublicCommitResult &opResult) {
        TxConsistencyMetadata cdata;
        std::vector<std::string> keySet;
        std::vector<std::string> valueSet;

        cdata.txId = opArg.id();

        /* A read-only transaction still needs to be commited in order to clean-up */
        if (opArg.key_size() != 0) {

            for (int i = 0; i < opArg.key_size(); ++i) {
                keySet.push_back(opArg.key(i));
                valueSet.push_back(opArg.value(i));
            }

            const PbPhysicalTimeSpec &v = opArg.lct();
            cdata.CT.Seconds = v.seconds();
            cdata.CT.NanoSeconds = v.nanoseconds();
        }
        bool ret = _coordinator->TxCommit(cdata, keySet, valueSet);
        assert(ret == true);

        opResult.set_succeeded(ret);
        if (ret) {
#ifdef DEBUG_MSGS
            SLOG((boost::format("TXID : %d committed with commitTime = %s") % cdata.txId %
                  Utils::physicaltime2str(cdata.CT).c_str()).str());
#endif
            opResult.mutable_ct()->set_seconds(cdata.CT.Seconds);
            opResult.mutable_ct()->set_nanoseconds(cdata.CT.NanoSeconds);
        } else {
            SLOG((boost::format("TXID : %d FAILED") % cdata.txId).str());
            throw KVOperationException("Transaction commit failed.");
        }
    }

#endif

#ifdef CURE

    void KVTxServer::HandleMyPut(PbRpcCureKVPublicWriteArg &opArg, PbRpcCureKVPublicWriteResult &opResult) {
        TxConsistencyMetadata cdata;
        std::vector<std::string> keySet;
        std::vector<std::string> valueSet;

        cdata.txId = opArg.id();

        if (opArg.key_size() != 0) {
            for (int i = 0; i < opArg.key_size(); ++i) {
                keySet.push_back(opArg.key(i));
                valueSet.push_back(opArg.value(i));
            }
        }

        bool ret = _coordinator->MyPut(cdata, keySet, valueSet);
		Transaction *tx = _coordinator->GetTransaction(cdata.txId);

        opResult.set_succeeded(ret);
        if (ret) {
        	opResult.mutable_ut()->set_seconds(tx->txContex->DV[_replicaId].Seconds);
        	opResult.mutable_ut()->set_nanoseconds(tx->txContex->DV[_replicaId].NanoSeconds);
        }
    }

#endif

#ifdef TUNABLE_CAUSAL

    void KVTxServer::HandleTcPut(PbRpcTcCureKVPublicWriteArg &opArg, PbRpcTcCureKVPublicWriteResult &opResult) {
        TxConsistencyMetadata cdata;
        std::vector<std::string> keySet;
        std::vector<std::string> valueSet;
	
        cdata.txId = opArg.id();
		int level = opArg.level();

        if (opArg.key_size() != 0) {
            for (int i = 0; i < opArg.key_size(); ++i) {
                keySet.push_back(opArg.key(i));
                valueSet.push_back(opArg.value(i));
            }
        }

#ifdef MYDEBUG
		//printf("KVServer.HandleTcPut. TcPut %s %s %d\n", keySet[0].c_str(), valueSet[0].c_str(), level);
		SLOG((boost::format("KVServer.HandleTcPut. TcPut %s %s %d\n") %keySet[0].c_str() %valueSet[0].c_str() %level).str());
#endif
        bool ret = _coordinator->TcPut(cdata, keySet, valueSet, level);
		Transaction *tx = _coordinator->GetTransaction(cdata.txId);

        opResult.set_succeeded(ret);
        if (ret) {
#ifdef DEBUG_MSGS
			std::string dvStr = "[";
        	for (int i = 0; i < tx->txContex->DV.size(); i++) {
        	     dvStr += (boost::format("%s,") % Utils::physicaltime2str(tx->txContex->DV[i])).str();
        	}
          	dvStr += "]";
       		SLOG((boost::format("Done: op (put %s %s) with ut = %s") %keySet[0]  %valueSet[0] %dvStr).str());
#endif
        	opResult.mutable_ut()->set_seconds(tx->txContex->DV[_replicaId].Seconds);
        	opResult.mutable_ut()->set_nanoseconds(tx->txContex->DV[_replicaId].NanoSeconds);      
        }
    }

#endif

#ifdef CURE
    void KVTxServer::HandleTxCommit(PbRpcTccCureKVPublicCommitArg &opArg, PbRpcTccCureKVPublicCommitResult &opResult) {
        TxConsistencyMetadata cdata;
        std::vector<std::string> keySet;
        std::vector<std::string> valueSet;

        cdata.txId = opArg.id();

        /* A read-only transaction still needs to be commited in order to clean-up */
        if (opArg.key_size() != 0) {


            for (int i = 0; i < opArg.key_size(); ++i) {
                keySet.push_back(opArg.key(i));
                valueSet.push_back(opArg.value(i));
            }
        }

        bool ret = _coordinator->TxCommit(cdata, keySet, valueSet);

        opResult.set_succeeded(ret);
        if (ret) {
#ifdef DEBUG_MSGS
            SLOG((boost::format("TXID : %d committed with commitTime = %s") % cdata.txId %
                  Utils::physicaltime2str(cdata.CT).c_str()).str());
#endif

            for (int j = 0; j < cdata.CT.size(); j++) {
                PbPhysicalTimeSpec *ct = opResult.add_ct();
                ct->set_seconds(cdata.CT[j].Seconds);
                ct->set_nanoseconds(cdata.CT[j].NanoSeconds);
            }
        }
    }

#endif

    void KVTxServer::ServePartitionConnection() {
        try {
            TCPServerSocket serverSocket(_partitionPort);
            for (;;) {
                TCPSocket *clientSocket = serverSocket.accept();
                std::thread t(&KVTxServer::HandlePartitionRequest, this, clientSocket);
                t.detach();
            }
        } catch (SocketException &e) {
            fprintf(stdout, "Server partition socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "SEVERE Server partition socket exception: %s\n", "...");
            fflush(stdout);
        }
    }


    void KVTxServer::ServePartitionsWriteConnection() {
        try {
            TCPServerSocket serverSocket(_partitionPort + 100);
            for (;;) {
                TCPSocket *clientSocket = serverSocket.accept();
                std::thread t(&KVTxServer::HandlePartitionsWriteRequest, this, clientSocket);
                t.detach();
            }
        } catch (SocketException &e) {
            fprintf(stdout, "Server partition writes socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "SEVERE Server partition writes socket exception: %s\n", "...");
            fflush(stdout);
        }
    }


    void KVTxServer::HandlePartitionRequest(TCPSocket *clientSocket) {
        try {
            RPCServerPtr rpcServer(new RPCServer(clientSocket));
            DBPartition servedPartition;

            for (;;) {
                // get rpc request
                PbRpcRequest rpcRequest;
                PbRpcReply rpcReply;
                rpcServer->RecvRequest(rpcRequest);

                switch (static_cast<RPCMethod> (rpcRequest.methodid())) {

                    case RPCMethod::InternalShowItem: {
                        // get arguments
                        PbRpcKVInternalShowItemArg opArg;
                        PbRpcKVInternalShowItemResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());
                        // execute op
                        std::string itemVersions;
                        bool ret = _coordinator->ShowItem(opArg.key(), itemVersions);
                        opResult.set_succeeded(ret);
                        if (ret) {
                            opResult.set_itemversions(itemVersions);
                        }
                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);
                        break;
                    }

                    case RPCMethod::InitializePartitioning: {
                        PbPartition opArg;
                        opArg.ParseFromString(rpcRequest.arguments());

                        servedPartition.Name = opArg.name();
                        servedPartition.PublicPort = opArg.publicport();
                        servedPartition.PartitionPort = opArg.partitionport();
                        servedPartition.ReplicationPort = opArg.replicationport();
                        servedPartition.PartitionId = opArg.partitionid();
                        servedPartition.ReplicaId = opArg.replicaid();
                        break;
                    }

                    case RPCMethod::InternalTxSliceReadKeys: {
#ifdef H_CURE
                        PbRpcTccWrenKVInternalTxSliceReadKeysArg opArg;
                        PbRpcTccWrenKVInternalTxSliceReadKeysResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());

#ifdef MEASURE_STATISTICS
                        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
#endif
                        HandleInternalTxSliceReadKeys<PbRpcTccWrenKVInternalTxSliceReadKeysArg, PbRpcTccWrenKVInternalTxSliceReadKeysResult>(
                                opArg, opResult);

#ifdef MEASURE_STATISTICS
                        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                        double duration = (endTime - startTime).toMilliSeconds();
                        SysStats::CohordHandleInternalSliceReadLatencySum = SysStats::CohordHandleInternalSliceReadLatencySum + duration;
                        SysStats::NumCohordHandleInternalSliceReadLatencyMeasurements++;
#endif
                        opResult.set_id(opArg.id());
                        opResult.set_src(_partitionId);
                        int src = opArg.src();

#ifdef MEASURE_STATISTICS
                        startTime = Utils::GetCurrentClockTime();
#endif
                        _coordinator->template C_SendInternalTxSliceReadKeysResult<PbRpcTccWrenKVInternalTxSliceReadKeysResult>(
                                opResult, src);

#ifdef MEASURE_STATISTICS
                        endTime = Utils::GetCurrentClockTime();
                        duration = (endTime - startTime).toMilliSeconds();
                        SysStats::CohordSendingReadResultsLatencySum = SysStats::CohordSendingReadResultsLatencySum + duration;
                        SysStats::NumCohordSendingReadResultsLatencyMeasurements++;
#endif

#elif defined(WREN)
                        PbRpcTccNBWrenKVInternalTxSliceReadKeysArg opArg;
                        PbRpcTccNBWrenKVInternalTxSliceReadKeysResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());

#ifdef MEASURE_STATISTICS
                        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
#endif

                        HandleInternalTxSliceReadKeys<PbRpcTccNBWrenKVInternalTxSliceReadKeysArg, PbRpcTccNBWrenKVInternalTxSliceReadKeysResult>(
                                opArg, opResult);

#ifdef MEASURE_STATISTICS
                        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                        double duration = (endTime - startTime).toMilliSeconds();
                        SysStats::CohordHandleInternalSliceReadLatencySum =
                                SysStats::CohordHandleInternalSliceReadLatencySum + duration;
                        SysStats::NumCohordHandleInternalSliceReadLatencyMeasurements++;
#endif

                        opResult.set_id(opArg.id());
                        opResult.set_src(_partitionId);
                        int src = opArg.src();

#ifdef MEASURE_STATISTICS
                        startTime = Utils::GetCurrentClockTime();
#endif
                        _coordinator->template C_SendInternalTxSliceReadKeysResult<PbRpcTccNBWrenKVInternalTxSliceReadKeysResult>(
                                opResult, src);

#ifdef MEASURE_STATISTICS
                        endTime = Utils::GetCurrentClockTime();
                        duration = (endTime - startTime).toMilliSeconds();
                        SysStats::CohordSendingReadResultsLatencySum =
                                SysStats::CohordSendingReadResultsLatencySum + duration;
                        SysStats::NumCohordSendingReadResultsLatencyMeasurements++;
#endif
#elif defined(CURE)

                        PbRpcTccCureKVInternalTxSliceReadKeysArg opArg;
                        PbRpcTccCureKVInternalTxSliceReadKeysResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());

#ifdef MEASURE_STATISTICS
                        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
#endif

                        HandleInternalTxSliceReadKeys<PbRpcTccCureKVInternalTxSliceReadKeysArg, PbRpcTccCureKVInternalTxSliceReadKeysResult>(
                                opArg, opResult);
#ifdef MEASURE_STATISTICS
                        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                        double duration = (endTime - startTime).toMilliSeconds();
                        SysStats::CohordHandleInternalSliceReadLatencySum = SysStats::CohordHandleInternalSliceReadLatencySum + duration;
                        SysStats::NumCohordHandleInternalSliceReadLatencyMeasurements++;
#endif

                        opResult.set_id(opArg.id());
                        opResult.set_src(_partitionId);
                        int src = opArg.src();

#ifdef MEASURE_STATISTICS
                        startTime = Utils::GetCurrentClockTime();
#endif

                        _coordinator->template C_SendInternalTxSliceReadKeysResult<PbRpcTccCureKVInternalTxSliceReadKeysResult>(
                                opResult, src);

#ifdef MEASURE_STATISTICS
                        endTime = Utils::GetCurrentClockTime();
                        duration = (endTime - startTime).toMilliSeconds();
                        SysStats::CohordSendingReadResultsLatencySum = SysStats::CohordSendingReadResultsLatencySum + duration;
                        SysStats::NumCohordSendingReadResultsLatencyMeasurements++;
#endif
#endif
                        break;
                    }

#if defined(CURE)
                    case RPCMethod::InternalSliceReadKeys: {
                        PbRpcTccCureKVInternalTxSliceReadKeysArg opArg;
                        //PbRpcTccCureKVInternalTxSliceReadKeysResult opResult;
						PbRpcCureKVInternalSliceReadKeysResult opResult; // value and timestamp
                        opArg.ParseFromString(rpcRequest.arguments());

#ifdef MEASURE_STATISTICS
                        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
#endif

                        HandleInternalSliceReadKeys<PbRpcTccCureKVInternalTxSliceReadKeysArg, PbRpcCureKVInternalSliceReadKeysResult>(
                                opArg, opResult);
#ifdef MEASURE_STATISTICS
                        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                        double duration = (endTime - startTime).toMilliSeconds();
                        SysStats::CohordHandleInternalSliceReadLatencySum = SysStats::CohordHandleInternalSliceReadLatencySum + duration;
                        SysStats::NumCohordHandleInternalSliceReadLatencyMeasurements++;
#endif

                        opResult.set_id(opArg.id());
                        opResult.set_src(_partitionId);
                        int src = opArg.src();

#ifdef MEASURE_STATISTICS
                        startTime = Utils::GetCurrentClockTime();
#endif

                        //_coordinator->template C_SendInternalTxSliceReadKeysResult<PbRpcTccCureKVInternalTxSliceReadKeysResult>(
                        //        opResult, src);
						_coordinator->template C_SendInternalSliceReadKeysResult<PbRpcCureKVInternalSliceReadKeysResult>(
                                opResult, src);

#ifdef MEASURE_STATISTICS
                        endTime = Utils::GetCurrentClockTime();
                        duration = (endTime - startTime).toMilliSeconds();
                        SysStats::CohordSendingReadResultsLatencySum = SysStats::CohordSendingReadResultsLatencySum + duration;
                        SysStats::NumCohordSendingReadResultsLatencyMeasurements++;
#endif

                        break;
                    }

                    case RPCMethod::InternalSliceReadKeysResult: {
                        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
                        PbRpcCureKVInternalSliceReadKeysResult opResult; // including key and timestamp
                        opResult.ParseFromString(rpcRequest.arguments());
                        Transaction *tx = _coordinator->GetTransaction(opResult.id());
						
                        int part = (int) opResult.src();  // read from which partition
                        TxReadSlice *slice = tx->partToReadSliceMap[part];
                        slice->values.clear();
                        slice->values.resize(opResult.value_size(), "");

                        for (int i = 0; i < opResult.value_size(); i++) {
                            slice->values[i] = opResult.value(i);
                        }

						// set the timestamp in slice
						for (int i = 0; i < opResult.ut_size(); i++) {
							const PbPhysicalTimeSpec &v = opResult.ut(i);    
							PhysicalTimeSpec p(v.seconds(), v.nanoseconds());                       
							slice->ut.push_back(p);
							tx->txContex->DV[i] = MAX(p, tx->txContex->DV[i]);
                        }

                        slice->txWaitOnReadTime = opResult.waitedxact();
                        slice->sucesses = opResult.succeeded();
                        tx->readSlicesWaitHandle->SetIfCountZero();

                        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                        double duration = (endTime - startTime).toMilliSeconds();
                        SysStats::CoordinatorReadReplyHandlingLatencySum =
                                SysStats::CoordinatorReadReplyHandlingLatencySum + duration;
                        SysStats::NumCoordinatorReadReplyHandlingMeasurements++;
                        break;
                    }
#endif

#if defined(TUNABLE_CAUSAL)
                    case RPCMethod::TcInternalSliceReadKeys: {
                        PbRpcTcCureKVInternalSliceReadKeysArg opArg;
                        //PbRpcTccCureKVInternalTxSliceReadKeysResult opResult;
						PbRpcTcCureKVInternalSliceReadKeysResult opResult; // value, timestamp and css
                        opArg.ParseFromString(rpcRequest.arguments());

#ifdef MEASURE_STATISTICS
                        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
#endif

                        HandleTcInternalSliceReadKeys(opArg, opResult);

#ifdef MEASURE_STATISTICS
                        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                        double duration = (endTime - startTime).toMilliSeconds();
                        SysStats::CohordHandleInternalSliceReadLatencySum = SysStats::CohordHandleInternalSliceReadLatencySum + duration;
                        SysStats::NumCohordHandleInternalSliceReadLatencyMeasurements++;
#endif

                        opResult.set_id(opArg.id());
                        opResult.set_src(_partitionId);
                        int src = opArg.src();

#ifdef MEASURE_STATISTICS
                        startTime = Utils::GetCurrentClockTime();
#endif

                        //_coordinator->template C_SendInternalTxSliceReadKeysResult<PbRpcTccCureKVInternalTxSliceReadKeysResult>(
                        //        opResult, src);
						_coordinator->template C_SendInternalSliceReadKeysResult<PbRpcTcCureKVInternalSliceReadKeysResult>(
                                opResult, src);
#ifdef MYDEBUG
						SLOG("[MYDEBUG] Done _coordinator->template C_SendInternalSliceReadKeysResult");
#endif

#ifdef MEASURE_STATISTICS
                        endTime = Utils::GetCurrentClockTime();
                        duration = (endTime - startTime).toMilliSeconds();
                        SysStats::CohordSendingReadResultsLatencySum = SysStats::CohordSendingReadResultsLatencySum + duration;
                        SysStats::NumCohordSendingReadResultsLatencyMeasurements++;
#endif

                        break;
                    }

                    case RPCMethod::TcInternalSliceReadKeysResult: {
#ifdef MYDEBUG
						SLOG("[MYDEBUG] Starting RPCMethod::TcInternalSliceReadKeysResult");
#endif
                        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
                        PbRpcTcCureKVInternalSliceReadKeysResult opResult; // including key, timestamp and css
                        opResult.ParseFromString(rpcRequest.arguments());
                        Transaction *tx = _coordinator->GetTransaction(opResult.id());
#ifdef MYDEBUG
						SLOG("[MYDEBUG] Done getting transaction");
#endif						
                        int part = (int) opResult.src();  // read from which partition
                        TxReadSlice *slice = tx->partToReadSliceMap[part];
#ifdef MYDEBUG
						SLOG((boost::format("[MYDEBUG] tx->partToReadSliceMap[%d]") %part).str());
						if (!slice) SLOG("[MYDEBUG] slice is null");
						assert(slice);
						SLOG((boost::format("[MYDEBUG] Done getting slice of partition %d") %part).str());
						slice->values.push_back("");
						SLOG((boost::format("[MYDEBUG] slice->values.size = %d") %slice->values.size()).str());
#endif
	                    slice->values.clear();
#ifdef MYDEBUG
						SLOG("[MYDEBUG] Done clearing slice->values");
						SLOG((boost::format("[MYDEBUG] opResult.value_size = %d") %opResult.value_size()).str());
#endif	
                        slice->values.resize(opResult.value_size(), "");
#ifdef MYDEBUG
						SLOG("[MYDEBUG] Done resizing slice->values");
						SLOG((boost::format("[MYDEBUG] slice->values.size = %d") %slice->values.size()).str());
#endif
                        for (int i = 0; i < opResult.value_size(); i++) {
                            slice->values[i] = opResult.value(i);
                        }
#ifdef MYDEBUG
						SLOG("[MYDEBUG] Done setting slice->values and Start setting slice->ut");
#endif						
						// set the timestamp in slice]
						PhysicalTimeSpec t(0,0);
						slice->ut.resize(opResult.ut_size(),t);
#ifdef MYDEBUG
						SLOG("[MYDEBUG] Done clearing slice->ut");
#endif	
						for (int i = 0; i < opResult.ut_size(); i++) {
							const PbPhysicalTimeSpec &v = opResult.ut(i);    
							PhysicalTimeSpec p(v.seconds(), v.nanoseconds());  
#ifdef MYDEBUG
							std::string dvStr = "[";
        					dvStr += (boost::format("%s,") % Utils::physicaltime2str(p)).str();
        					dvStr += "]";
							SLOG((boost::format("[MYDEBUG] Got value = %s with ut[%d] = %s") %slice->values[0].c_str() %i %dvStr.c_str()).str());
#endif                     
							slice->ut[i] = p;
							// update hrvc and rdv in tx->txcontex
							// tx->txContex.DV[i] = MAX(p, tx->txContex.DV[i]);
							const PbPhysicalTimeSpec &vv = opResult.css(i); 
							PhysicalTimeSpec t(vv.seconds(), vv.nanoseconds());   
#ifdef MYDEBUG 
							SLOG("[MYDEBUG] Start setting tx->txContex->HRVC(RDV)");
#endif                        
							tx->txContex->HRVC[i] = MAX(tx->txContex->HRVC[i], t); 
							tx->txContex->RDV[i] = MAX(tx->txContex->RDV[i], p); 
                        }
#ifdef MYDEBUG
						SLOG("[MYDEBUG] Done setting tx->txContex->HRVC(RDV)");
						std::string dvStr = "[";
        				for (int i = 0; i < tx->txContex->HRVC.size(); i++) {
        					dvStr += (boost::format("%s,") % Utils::physicaltime2str(tx->txContex->HRVC[i])).str();
        				}
        				dvStr += "]";
						std::string dvStr1 = "[";
        				for (int i = 0; i < tx->txContex->RDV.size(); i++) {
        					dvStr1 += (boost::format("%s,") % Utils::physicaltime2str(tx->txContex->RDV[i])).str();
        				}
        				dvStr1 += "]";
						SLOG((boost::format("[MYDEBUG] Got value = %s with updated HRVC = %s RDV = %s") %slice->values[0].c_str() %dvStr.c_str() %
									dvStr1.c_str()).str());
#endif
                        slice->txWaitOnReadTime = opResult.waitedxact();
                        slice->sucesses = opResult.succeeded();
                        tx->readSlicesWaitHandle->SetIfCountZero();

                        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                        double duration = (endTime - startTime).toMilliSeconds();
                        SysStats::CoordinatorReadReplyHandlingLatencySum =
                                SysStats::CoordinatorReadReplyHandlingLatencySum + duration;
                        SysStats::NumCoordinatorReadReplyHandlingMeasurements++;
                        break;
                    }
#endif


#ifdef WREN
                    case RPCMethod::InternalTxSliceReadKeysResult: {

                        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();

                        PbRpcTccNBWrenKVInternalTxSliceReadKeysResult opResult;
                        opResult.ParseFromString(rpcRequest.arguments());
                        Transaction *tx = _coordinator->GetTransaction(opResult.id());
                        int part = opResult.src();

                        TxReadSlice *slice = tx->partToReadSliceMap[part];
                        slice->values.clear();
                        slice->values.resize(opResult.value_size(), "");

                        for (int i = 0; i < opResult.value_size(); i++) {
                            slice->values[i] = opResult.value(i);
                        }

                        slice->sucesses = opResult.succeeded();
                        tx->readSlicesWaitHandle->SetIfCountZero();

                        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                        double duration = (endTime - startTime).toMilliSeconds();
                        SysStats::CoordinatorReadReplyHandlingLatencySum =
                                SysStats::CoordinatorReadReplyHandlingLatencySum + duration;
                        SysStats::NumCoordinatorReadReplyHandlingMeasurements++;
                        break;
                    }

#else
                    case RPCMethod::InternalTxSliceReadKeysResult: {
                        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
                        PbRpcTccWrenKVInternalTxSliceReadKeysResult opResult;
                        opResult.ParseFromString(rpcRequest.arguments());
                        Transaction *tx = _coordinator->GetTransaction(opResult.id());
					
                        int part = (int) opResult.src();
                        TxReadSlice *slice = tx->partToReadSliceMap[part];
                        slice->values.clear();
                        slice->values.resize(opResult.value_size(), "");


                        for (int i = 0; i < opResult.value_size(); i++) {
                            slice->values[i] = opResult.value(i);
                        }
                        slice->txWaitOnReadTime = opResult.waitedxact();
                        slice->sucesses = opResult.succeeded();
                        tx->readSlicesWaitHandle->SetIfCountZero();

                        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                        double duration = (endTime - startTime).toMilliSeconds();
                        SysStats::CoordinatorReadReplyHandlingLatencySum =
                                SysStats::CoordinatorReadReplyHandlingLatencySum + duration;
                        SysStats::NumCoordinatorReadReplyHandlingMeasurements++;
                        break;
                    }
#endif

#ifdef H_CURE
                    case RPCMethod::SendRST: {
                        PbRpcRST pb_rst;
                        std::string arg;
                        pb_rst.ParseFromString(arg = rpcRequest.arguments());


#ifdef MEASURE_STATISTICS
                        SysStats::NumRecvRSTBytes += arg.size();
                        SysStats::NumRecvRSTs++;
#endif
                        PhysicalTimeSpec rst;
                        rst.Seconds = pb_rst.time().seconds();
                        rst.NanoSeconds = pb_rst.time().nanoseconds();
                        int peerId = pb_rst.srcpartition();
                        _coordinator->HandleRSTFromPeer(rst, peerId);
                        break;
                    }
#endif

#ifdef WREN
                    case RPCMethod::SendStabilizationTimes: {
                        PbRpcPeerStabilizationTimes pb_st;
                        std::string arg;
                        pb_st.ParseFromString(arg = rpcRequest.arguments());

#ifdef MEASURE_STATISTICS
                        SysStats::NumRecvRSTBytes += arg.size();
                        SysStats::NumRecvRSTs++;
#endif
                        PhysicalTimeSpec lst, rst;
                        lst.Seconds = pb_st.lst().seconds();
                        lst.NanoSeconds = pb_st.lst().nanoseconds();
                        rst.Seconds = pb_st.rst().seconds();
                        rst.NanoSeconds = pb_st.rst().nanoseconds();
                        int peerId = pb_st.srcpartition();

                        _coordinator->HandleStabilizationTimesFromPeer(lst, rst, peerId);
                        break;
                    }
#endif


                    case RPCMethod::SendLST: {
#ifdef WREN

                        PbRpcST pb_st;
                        std::string arg;
                        pb_st.ParseFromString(arg = rpcRequest.arguments());
#else

                        PbRpcLST pb_st;
                        std::string arg;
                        pb_st.ParseFromString(arg = rpcRequest.arguments());
#endif

#ifdef MEASURE_STATISTICS
                        SysStats::NumRecvGSTs += 1;
                        SysStats::NumRecvGSTBytes += arg.size();
#endif

                        PhysicalTimeSpec lst;

#ifdef WREN
                        PhysicalTimeSpec rst;
                        lst.Seconds = pb_st.lst().seconds();
                        lst.NanoSeconds = pb_st.lst().nanoseconds();
                        rst.Seconds = pb_st.rst().seconds();
                        rst.NanoSeconds = pb_st.rst().nanoseconds();

#else
                        lst.Seconds = pb_st.time().seconds();
                        lst.NanoSeconds = pb_st.time().nanoseconds();
#endif

                        int round = pb_st.round();

                        if (SysConfig::GSTDerivationMode == GSTDerivationType::TREE) {
#ifdef WREN
                            _coordinator->HandleLSTFromChildren(lst, rst, round);

#elif defined(H_CURE)

                            _coordinator->HandleLSTFromChildren(lst, round);
#endif


                        } else {
                            std::cout << "Unexisting GSTDerivationType: \n";
                        }
                        break;
                    }


                    case RPCMethod::SendGST: {

#if defined(H_CURE)
                        PbRpcRST pb_st;
                        std::string arg;
                        pb_st.ParseFromString(arg = rpcRequest.arguments());
#ifdef MEASURE_STATISTICS
                        SysStats::NumRecvGSTBytes += arg.size();
                        SysStats::NumRecvGSTs++;
#endif

                        PhysicalTimeSpec rst;
                        rst.Seconds = pb_st.time().seconds();
                        rst.NanoSeconds = pb_st.time().nanoseconds();

                        _coordinator->HandleGSTFromParent(rst);

#elif defined(WREN)
                        PbRpcStabilizationTime pb_st;
                        std::string arg;
                        pb_st.ParseFromString(arg = rpcRequest.arguments());
#ifdef MEASURE_STATISTICS
                        SysStats::NumRecvGSTBytes += arg.size();
                        SysStats::NumRecvGSTs++;
#endif
                        PhysicalTimeSpec rst, lst;
                        lst.Seconds = pb_st.glst().seconds();
                        lst.NanoSeconds = pb_st.glst().nanoseconds();

                        rst.Seconds = pb_st.grst().seconds();
                        rst.NanoSeconds = pb_st.grst().nanoseconds();

                        _coordinator->HandleGSTFromParent(lst, rst);
#endif

                        break;
                    }
#ifdef CURE
                    case RPCMethod::SendPVV: {
                        PbRpcPVV pb_lst;
                        std::string arg;
                        pb_lst.ParseFromString(arg = rpcRequest.arguments());
#ifdef MEASURE_STATISTICS
                        SysStats::NumRecvGSTs += 1;
                        SysStats::NumRecvGSTBytes += arg.size();
#endif

                        std::vector<PhysicalTimeSpec> inputPVV;
                        for (int i = 0; i < pb_lst.pvv().size(); i++) {
                            const PbPhysicalTimeSpec &v = pb_lst.pvv(i);
                            PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
                            inputPVV.push_back(p);
                        }
                        int round = pb_lst.round();
                        if (SysConfig::GSTDerivationMode == GSTDerivationType::TREE) {
                            _coordinator->HandlePVVFromChildren(inputPVV, round);
                        } else {
                            assert(false);
                        }
                        break;
                    }

                    case RPCMethod::SendGSV: {
                        PbRpcGSV pb_gst;
                        std::string arg;
                        pb_gst.ParseFromString(arg = rpcRequest.arguments());
#ifdef MEASURE_STATISTICS
                        SysStats::NumRecvGSTBytes += arg.size();
                        SysStats::NumRecvGSTs++;
#endif
                        std::vector<PhysicalTimeSpec> inputGSV;
                        for (int i = 0; i < pb_gst.gsv().size(); i++) {
                            const PbPhysicalTimeSpec &v = pb_gst.gsv(i);
                            PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
                            inputGSV.push_back(p);
                        }
                        _coordinator->HandleGSVFromParent(inputGSV);
                        break;
                    }

#endif //CURE


                    default:
                        throw KVOperationException("(partition) Unsupported operation.");
                }
            }
        } catch (SocketException &e) {

            fprintf(stdout, "Partition serving thread socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "SEVERE Server partition socket exception: %s\n", "...");
            fflush(stdout);
        }


    }


    void KVTxServer::HandlePartitionsWriteRequest(TCPSocket *clientSocket) {
        try {
            RPCServerPtr rpcServer(new RPCServer(clientSocket));
            DBPartition servedPartition;

            for (;;) {
                // get rpc request
                PbRpcRequest rpcRequest;
                PbRpcReply rpcReply;
                rpcServer->RecvRequest(rpcRequest);

                switch (static_cast<RPCMethod> (rpcRequest.methodid())) {

                    case RPCMethod::InternalWriteRequest: {
#if defined(CURE)
                        PbRpcCurePartitionClientWriteRequestArg opArg;
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleInternalWriteRequest<PbRpcCurePartitionClientWriteRequestArg>(opArg);
#endif
                        break;
                    }

                    case RPCMethod::InternalPrepareRequest: {
#ifdef H_CURE
                        PbRpcTccWrenPartitionClientPrepareRequestArg opArg;
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleInternalPrepareRequest<PbRpcTccWrenPartitionClientPrepareRequestArg>(opArg);

#elif defined(WREN)
                        PbRpcTccNBWrenPartitionClientPrepareRequestArg opArg;
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleInternalPrepareRequest<PbRpcTccNBWrenPartitionClientPrepareRequestArg>(opArg);

#elif defined(CURE)
                        PbRpcTccCurePartitionClientPrepareRequestArg opArg;
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleInternalPrepareRequest<PbRpcTccCurePartitionClientPrepareRequestArg>(opArg);
#endif
                        break;
                    }

                    case RPCMethod::InternalWriteReply: {
                        //You already got the result from the queried partition
                        //PbRpcKVInternalPrepareReplyResult opResult;
						PbRpcPartitionClientWriteRequestResult opResult;
                        opResult.ParseFromString(rpcRequest.arguments());

                        PhysicalTimeSpec pt;
                        pt.Seconds = opResult.ut().seconds();
                        pt.NanoSeconds = opResult.ut().nanoseconds();

                        _coordinator->HandleInternalWriteReply(opResult.id(), pt);

                        break;
                    }

                    case RPCMethod::InternalPrepareReply: {
                        //You already got the result from the queried partition
                        PbRpcKVInternalPrepareReplyResult opResult;
                        opResult.ParseFromString(rpcRequest.arguments());

                        PhysicalTimeSpec pt;
                        pt.Seconds = opResult.pt().seconds();
                        pt.NanoSeconds = opResult.pt().nanoseconds();

                        _coordinator->HandleInternalPrepareReply(opResult.id(), opResult.src(), pt,
                                                                 opResult.blockduration());

                        break;
                    }

                    case RPCMethod::InternalCommitRequest: {

#if defined(H_CURE) || defined(WREN)
                        PbRpCommitRequestArg opArg;
                        PhysicalTimeSpec ct;

                        opArg.ParseFromString(rpcRequest.arguments());
                        unsigned int txId = opArg.id();
                        ct.Seconds = opArg.ct().seconds();
                        ct.NanoSeconds = opArg.ct().nanoseconds();

                        _coordinator->HandleInternalCommitRequest(txId, ct);
#elif defined(CURE)
                        PbRpTccCureCommitRequestArg opArg;
                        std::vector<PhysicalTimeSpec> ct;

                        opArg.ParseFromString(rpcRequest.arguments());
                        unsigned int txId = opArg.id();

                        const int numReplicas = _coordinator->NumReplicasPerPartition();
                        assert(numReplicas == opArg.ct_size());
                        for (int i = 0; i < numReplicas; i++) {
                            const PbPhysicalTimeSpec &v = opArg.ct(i);
                            PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
                            ct.push_back(p);
                        }

                        _coordinator->HandleInternalCommitRequest(txId, ct);
#endif
                        break;
                    }
                    default:
                        throw KVOperationException("(partition) Unsupported operation.");
                }
            }
        } catch (SocketException &e) {

            fprintf(stdout, "Partition serving thread socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "SEVERE Server partition socket exception: %s\n", "...");
            fflush(stdout);
        }
    }

#ifdef WREN

    template<class Argument, class Result>
    void KVTxServer::HandleInternalTxSliceReadKeys(Argument &opArg, Result &opResult) {
        TxContex cdata;
        std::vector<std::string> readValues;
        std::vector<std::string> readKeys;
        bool ret;

        const PbPhysicalTimeSpec &v = opArg.lst();
        cdata.GLST.Seconds = v.seconds();
        cdata.GLST.NanoSeconds = v.nanoseconds();

        const PbPhysicalTimeSpec &w = opArg.rst();
        cdata.GRST.Seconds = w.seconds();
        cdata.GRST.NanoSeconds = w.nanoseconds();

        readValues.resize(opArg.key_size(), "");
        for (int i = 0; i < opArg.key_size(); ++i) {
            readKeys.push_back(opArg.key(i));
        }

        ret = _coordinator->InternalTxSliceReadKeys(opArg.id(), cdata.GLST, cdata.GRST, readKeys, readValues);

        opResult.set_succeeded(ret);

        if (ret) {
            for (int i = 0; i < opArg.key_size(); i++) {
                opResult.add_value(readValues[i]);
            }

        } else {
            fprintf(stdout, "Error in HandleInternalTxSliceReadKeys .\n");
            fflush(stdout);
        }
    }

#endif

#if defined(H_CURE)

    template<class Argument, class Result>
    void KVTxServer::HandleInternalTxSliceReadKeys(Argument &opArg, Result &opResult) {
        TxContex cdata;
        std::vector<std::string> readValues;
        std::vector<std::string> readKeys;
        bool ret;

        const PbPhysicalTimeSpec &v = opArg.ldt();
        cdata.LDT.Seconds = v.seconds();
        cdata.LDT.NanoSeconds = v.nanoseconds();

        const PbPhysicalTimeSpec &w = opArg.rst();
        cdata.RST.Seconds = w.seconds();
        cdata.RST.NanoSeconds = w.nanoseconds();

        readValues.resize(opArg.key_size(), "");
        for (int i = 0; i < opArg.key_size(); ++i) {
            readKeys.push_back(opArg.key(i));
        }

        ret = _coordinator->InternalTxSliceReadKeys(opArg.id(), cdata, readKeys, readValues);

        opResult.set_succeeded(ret);

        if (ret) {
            for (int i = 0; i < readValues.size(); i++) {
                opResult.add_value(readValues[i]);
#ifdef MEASURE_STATISTICS
                assert(cdata.waited_xact >= 0);
                opResult.set_waitedxact(cdata.waited_xact);
#endif
            }

        } else {
            fprintf(stdout, "Error in HandleInternalTxSliceReadKeys .\n");
            fflush(stdout);
        }
    }

#endif

#ifdef CURE

    template<class Argument, class Result>
    void KVTxServer::HandleInternalTxSliceReadKeys(Argument &opArg, Result &opResult) {
        TxContex cdata;
        std::vector<std::string> readValues;
        std::vector<std::string> readKeys;
        bool ret;

        const int numReplicas = _coordinator->NumReplicasPerPartition();
        PhysicalTimeSpec t(0, 0);
        cdata.DV.resize(numReplicas, t);
        assert(cdata.DV.size() == numReplicas);

        for (int i = 0; i < numReplicas; i++) {
            const PbPhysicalTimeSpec &v = opArg.dv(i);
            PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
            cdata.DV[i] = p;
        }

        readValues.resize(opArg.key_size(), "");
        for (int i = 0; i < opArg.key_size(); ++i) {
            readKeys.push_back(opArg.key(i));
        }

        ret = _coordinator->InternalTxSliceReadKeys(opArg.id(), cdata, readKeys, readValues);

        opResult.set_succeeded(ret);

        if (ret) {
            for (int i = 0; i < readValues.size(); i++) {
                opResult.add_value(readValues[i]);
#ifdef MEASURE_STATISTICS
                assert(cdata.waited_xact >= 0);
                opResult.set_waitedxact(cdata.waited_xact);
#endif
            }

        } else {
            fprintf(stdout, "Error in HandleInternalTxSliceReadKeys .\n");
            fflush(stdout);
        }
    }
#endif

#ifdef TUNABLE_CAUSAL
    void KVTxServer::HandleTcInternalSliceReadKeys(PbRpcTcCureKVInternalSliceReadKeysArg &opArg, 
										PbRpcTcCureKVInternalSliceReadKeysResult &opResult) {
#ifdef MYDEBUG
		//printf("Strarting KVTxServer::HandleTcInternalSliceReadKeys\n");
		SLOG("Strarting KVTxServer::HandleTcInternalSliceReadKeys");
#endif
        TxContex cdata;
        std::vector<std::string> readValues;
        std::vector<std::string> readKeys;
		int level;

		const int numReplicas = _coordinator->NumReplicasPerPartition();

		std::vector<PhysicalTimeSpec> ut;
        bool ret;
       
        PhysicalTimeSpec t(0, 0);
        cdata.DV.resize(numReplicas, t); 
		ut.resize(numReplicas, t);
		assert(ut.size() == numReplicas);
        assert(cdata.DV.size() == numReplicas);

		// cdata.DV = max(oparg.rdv, oparg.wdv)
        for (int i = 0; i < numReplicas; ++i) {
            const PbPhysicalTimeSpec &v = opArg.rdv(i);
			const PbPhysicalTimeSpec &vv = opArg.wdv(i);
            PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
            PhysicalTimeSpec pt(vv.seconds(), vv.nanoseconds());
            cdata.DV[i] = MAX(p, pt);
        }

        readValues.resize(opArg.key_size(), "");
        for (int i = 0; i < opArg.key_size(); ++i) {
            readKeys.push_back(opArg.key(i));
		}

#ifdef MYDEBUG
		std::string dvStr = "[";
        for (int i = 0; i < cdata.DV.size(); i++) {
        	dvStr += (boost::format("%s,") % Utils::physicaltime2str(cdata.DV[i])).str();
        }
        dvStr += "]";

		//printf("KVTxServer::HandleTcInternalSliceReadKeys. (TcGet %s) with cvc = %s\n", readKeys[0].c_str(), dvStr.c_str());

        SLOG((boost::format("KVTxServer::HandleTcInternalSliceReadKeys. (TcGet %s) with cvc = %s") %readKeys[0].c_str() %dvStr.c_str()).str());
#endif
        ret = _coordinator->InternalSliceReadKeys(opArg.id(), cdata, readKeys, readValues, ut);		

        opResult.set_succeeded(ret);

        if (ret) {
            for (int i = 0; i < readValues.size(); i++) {
                opResult.add_value(readValues[i]);
#ifdef MEASURE_STATISTICS
                assert(cdata.waited_xact >= 0);    // CURE: 0, TUNABLE_CAUSAL: >=0 
                opResult.set_waitedxact(cdata.waited_xact);
#endif
            }

			std::vector<PhysicalTimeSpec> t = _coordinator->getGSV();

			for (int i = 0; i < numReplicas; i++) {
            	PbPhysicalTimeSpec *vv = opResult.add_ut();
            	vv->set_seconds(ut[i].Seconds);
            	vv->set_nanoseconds(ut[i].NanoSeconds);	

            	PbPhysicalTimeSpec *v = opResult.add_css();
            	v->set_seconds(t[i].Seconds);
            	v->set_nanoseconds(t[i].NanoSeconds);												
			}

#ifdef MYDEBUG
			SLOG("[MYDEBUG] Done KVTxServer::HandleTcInternalSliceReadKeys.");
			std::string dvStr = "[";
	        for (int i = 0; i < t.size(); i++) {
	        	dvStr += (boost::format("%s,") % Utils::physicaltime2str(t[i])).str();
    	    }
    	    dvStr += "]";
			std::string dvStr1 = "[";
    	    for (int i = 0; i < ut.size(); i++) {
    	    	dvStr1 += (boost::format("%s,") % Utils::physicaltime2str(ut[i])).str();
    	    }
    	    dvStr1 += "]";
    	    SLOG((boost::format("Return value = %s with ut = %s gss = %s") %opResult.value(0).c_str() %dvStr1.c_str() %dvStr.c_str()).str());
#endif
        } else {
            fprintf(stdout, "Error in HandleInternalSliceReadKeys .\n");
            fflush(stdout);
        }
    }
#endif

#ifdef CURE
    template<class Argument, class Result>
    void KVTxServer::HandleInternalSliceReadKeys(Argument &opArg, Result &opResult) {
        TxContex cdata;
        std::vector<std::string> readValues;
        std::vector<std::string> readKeys;

		const int numReplicas = _coordinator->NumReplicasPerPartition();

		std::vector<PhysicalTimeSpec> ut;
        bool ret;
       
        PhysicalTimeSpec t(0, 0);
        cdata.DV.resize(numReplicas, t);
		ut.resize(numReplicas, t);
		assert(ut.size() == numReplicas);
        assert(cdata.DV.size() == numReplicas);

        for (int i = 0; i < numReplicas; i++) {
            const PbPhysicalTimeSpec &v = opArg.dv(i);
            PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
            cdata.DV[i] = p;
        }

        readValues.resize(opArg.key_size(), "");
        for (int i = 0; i < opArg.key_size(); ++i) {
            readKeys.push_back(opArg.key(i));
        }

        ret = _coordinator->InternalSliceReadKeys(opArg.id(), cdata, readKeys, readValues, ut);		

        opResult.set_succeeded(ret);

        if (ret) {
            for (int i = 0; i < readValues.size(); i++) {
                opResult.add_value(readValues[i]);
#ifdef MEASURE_STATISTICS
                assert(cdata.waited_xact >= 0);    // always 0 
                opResult.set_waitedxact(cdata.waited_xact);
#endif
            }

			for (int i = 0; i < numReplicas; i++) {
            	PbPhysicalTimeSpec *vv = opResult.add_ut();
            	vv->set_seconds(ut[i].Seconds);
            	vv->set_nanoseconds(ut[i].NanoSeconds);								
			}

        } else {
            fprintf(stdout, "Error in HandleInternalSliceReadKeys .\n");
            fflush(stdout);
        }
    }

    template<class Argument>
    void KVTxServer::HandleInternalWriteRequest(Argument &opArg) {
        TxContex cdata;
        std::vector <std::string> keys;
        std::vector <std::string> vals;
        bool ret;

        int txId = opArg.id();

        const int numReplicas = _coordinator->NumReplicasPerPartition();
        if(numReplicas != opArg.dv_size()){
            SLOG((boost::format(
                                "numReplicas = %s  opArg.cvc_size() = %d\n")
                              % numReplicas
                              % opArg.dv_size()).str());
        }

        assert(numReplicas == opArg.dv_size());
        for (int i = 0; i < numReplicas; i++) {
            const PbPhysicalTimeSpec &v = opArg.dv(i);
            PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
            cdata.DV.push_back(p);
        }

        for (int i = 0; i < opArg.key_size(); ++i) {
            keys.push_back(opArg.key(i));
            vals.push_back(opArg.value(i));
        }
        int src = opArg.src();
		
#ifdef MYDEBUG
		std::string dvStr = "[";
        for (int i = 0; i < cdata.DV.size(); i++) {
        	dvStr += (boost::format("%s,") % Utils::physicaltime2str(cdata.DV[i])).str();
        }
        dvStr += "]";
		//printf("KVServer.HandleInternalWriteRequest. (TcPut %s %s) with cvc = %s\n", keys[0].c_str(),  vals[0].c_str(), dvStr.c_str());

        SLOG((boost::format("KVServer.HandleInternalWriteRequest. (TcPut %s %s) with cvc = %s") %keys[0].c_str()  %vals[0].c_str() %dvStr.c_str()).str());
#endif
        _coordinator->InternalWriteRequest(txId, cdata, keys, vals, src);
    }
#endif

    template<class Argument>
    void KVTxServer::HandleInternalPrepareRequest(Argument &opArg) {
        TxContex cdata;
        std::vector <std::string> keys;
        std::vector <std::string> vals;
        bool ret;

        int txId = opArg.id();

#ifdef CURE
        const int numReplicas = _coordinator->NumReplicasPerPartition();
        if(numReplicas != opArg.dv_size()){
            SLOG((boost::format(
                                "numReplicas = %s  opArg.gsv_size() = %d\n")
                              % numReplicas
                              % opArg.dv_size()).str());
        }

        assert(numReplicas == opArg.dv_size());
        for (int i = 0; i < numReplicas; i++) {
            const PbPhysicalTimeSpec &v = opArg.dv(i);
            PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
            cdata.DV.push_back(p);
        }

#endif

        for (int i = 0; i < opArg.key_size(); ++i) {
            keys.push_back(opArg.key(i));
            vals.push_back(opArg.value(i));
        }
        int src = opArg.src();

        _coordinator->InternalPrepareRequest(txId, cdata, keys, vals, src);
    }

    void KVTxServer::ServeReplicationConnection() {
        try {
            TCPServerSocket serverSocket(_replicationPort);
            for (;;) {
                TCPSocket *clientSocket = serverSocket.accept();
                std::thread t(&KVTxServer::HandleReplicationRequest, this, clientSocket);
                t.detach();
            }
        } catch (SocketException &e) {

            fprintf(stdout, "Server replication socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "SEVERE Server partition socket exception: %s\n", "...");
            fflush(stdout);
        }
    }


    void KVTxServer::HandleReplicationRequest(TCPSocket *clientSocket) {
        try {
            RPCServerPtr rpcServer(new RPCServer(clientSocket));
            DBPartition servedPartition;

            for (;;) {
                // get rpc request
                PbRpcRequest rpcRequest;
                rpcServer->RecvRequest(rpcRequest);
                switch (static_cast<RPCMethod> (rpcRequest.methodid())) {
                    case RPCMethod::InitializeReplication: {

                        PbPartition opArg;
                        opArg.ParseFromString(rpcRequest.arguments());

                        servedPartition.Name = opArg.name();
                        servedPartition.PublicPort = opArg.publicport();
                        servedPartition.PartitionPort = opArg.partitionport();
                        servedPartition.ReplicationPort = opArg.replicationport();
                        servedPartition.PartitionId = opArg.partitionid();
                        servedPartition.ReplicaId = opArg.replicaid();

                        break;
                    }

                    case RPCMethod::ReplicateUpdate: {
                        // get arguments
                        PbRpcReplicationArg opArg;
                        std::string arg;
                        opArg.ParseFromString(arg = rpcRequest.arguments());

#ifdef MEASURE_STATISTICS
                        SysStats::NumRecvReplicationBytes += arg.size();
#endif
                        // apply propagated updates
                        std::vector<PropagatedUpdate *> updates;

                        for (int i = 0; i < opArg.updaterecord_size(); i++) {
                            PropagatedUpdate *update = new PropagatedUpdate();

#if defined(H_CURE) || defined(WREN)
                            PbLogTccWrenSetRecord record;
#endif

#ifdef CURE
                            PbLogTccCureSetRecord record;
#endif


                            const std::string &serializedRecord = opArg.updaterecord(i);
                            record.ParseFromString(serializedRecord);
                            update->SerializedRecord = serializedRecord;

                            update->SrcReplica = servedPartition.ReplicaId;
                            update->Key = record.key();
                            update->Value = record.value();
                            update->UT.Seconds = record.ut().seconds();
                            update->UT.NanoSeconds = record.ut().nanoseconds();
                            update->LUT = record.lut();
#ifdef MEASURE_VISIBILITY_LATENCY
                            update->CreationTime.Seconds = record.creationtime().seconds();
                            update->CreationTime.NanoSeconds = record.creationtime().nanoseconds();
#endif

#ifdef CURE
                            PhysicalTimeSpec init;

                            update->DV.resize(_coordinator->NumReplicasPerPartition(), init);
                            assert(_coordinator->NumReplicasPerPartition() == record.dv_size());

                            for (int j = 0; j < _coordinator->NumReplicasPerPartition(); j++) {
                                const PbPhysicalTimeSpec &v = record.dv(j);
                                PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
                                update->DV[j] = p;
                            }

#elif defined(H_CURE)
                            update->RST.Seconds = record.rst().seconds();
                            update->RST.NanoSeconds = record.rst().nanoseconds();
#endif


                            updates.push_back(update);
                        }

                        _coordinator->HandlePropagatedUpdate(updates);

                        break;
                    }
                    case RPCMethod::SendHeartbeat: {

                        // get arguments
                        PbRpcHeartbeat pb_hb;

                        pb_hb.ParseFromString(rpcRequest.arguments());

                        Heartbeat hb;

                        hb.PhysicalTime.Seconds = pb_hb.physicaltime().seconds();
                        hb.PhysicalTime.NanoSeconds = pb_hb.physicaltime().nanoseconds();

                        hb.LogicalTime = pb_hb.logicaltime();

                        _coordinator->HandleHeartbeat(hb, servedPartition.ReplicaId);

                        break;
                    }

                    default:
                        throw KVOperationException("(replication) Unsupported operation\n");

                }
            }
        } catch (SocketException &e) {
            fprintf(stdout, "HandleReplicationRequest:Client serving thread socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "SEVERE Server partition socket exception: %s\n", "...");
            fflush(stdout);
        }
    }


} // namespace scc
