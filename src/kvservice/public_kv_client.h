#ifndef SCC_KVSERVICE_PUBLIC_TX_CLIENT_H
#define SCC_KVSERVICE_PUBLIC_TX_CLIENT_H

#include "common/types.h"
#include "rpc/sync_rpc_client.h"
#include "rpc/rpc_id.h"
#include <string>
#include <vector>
#include <unordered_set>

namespace scc {

    class PublicTxClient {
    public:
        PublicTxClient(std::string serverName, int serverPort);
        PublicTxClient(std::string serverName, int serverPort, int numCtxs);
        ~PublicTxClient();

        void Echo(const std::string &input, std::string &output);
		
		// for GET and PUT commands	
#ifdef CURE
        bool MyGet(const std::vector<std::string> &keySet, std::vector<std::string> &valueSet);
        bool MyPut(const std::vector<std::string> &keySet, std::vector<std::string> &valueSet);
		bool MyStart();
#endif

#ifdef TUNABLE_CAUSAL
        bool TcGet(const std::vector<std::string> &keySet, std::vector<std::string> &valueSet, int level);
        bool TcPut(const std::vector<std::string> &keySet, std::vector<std::string> &valueSet, int level);
#endif

        bool TxStart();
        bool TxRead(const std::vector<std::string> &keySet, std::vector<std::string> &valueSet);
        bool TxWrite(const std::vector<std::string> &keySet, std::vector<std::string> &valueSet);
        bool TxCommit();

        bool ShowItem(const std::string &key, std::string &itemVersions);
        bool ShowDB(std::string &allItemVersions); // TBD
        bool ShowState(std::string &stateStr);
        bool ShowStateCSV(std::string &stateStr);
        bool DumpLatencyMeasurement(std::string &resultStr); // TBD

        void resetSession();
        void setTotalNumKeyInKVStore(int count);
        int getLocalReplicaId();
        int getNumReplicas();

        int getNumItemsReadFromClientWriteSet();
        int getNumItemsReadFromClientReadSet();
        int getNumItemsReadFromClientWriteCache();
        int getNumItemsReadFromStore();
        int getTotalNumReadItems();
		int getTotalNumWriteItems();

    private:
        std::string _serverName;
        int _serverPort;
        SyncRPCClient *_rpcClient;
        int _numPartitions;
        int _numReplicasPerPartition;
        int _replicaId;
        int totalNumKeyInKVStore;
        TxClientMetadata _sessionData;
        int totalNumReadItems;
		int totalNumWriteItems;
        int numItemsReadFromClientWriteSet;
        int numItemsReadFromClientReadSet;
        int numItemsReadFromClientWriteCache;
        int numItemsReadFromStore;

    private:
        bool GetServerConfig();


#ifdef WREN
        void trimWriteCache(PhysicalTimeSpec t);
        bool TxRead2(const std::vector<std::string> &keySet, std::vector<std::string> &valueSet);

        void updateWriteCache(PhysicalTimeSpec ct);
#endif
    };

} // namespace scc

#endif
