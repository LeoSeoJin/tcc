#include "kvservice/public_kv_client.h"
#include "common/exceptions.h"
#include "common/sys_config.h"
#include "common/utils.h"
#include <string>
#include <stdlib.h>
#include <iostream>
#include <boost/algorithm/string.hpp>

using namespace scc;

void showUsage();

int main(int argc, char *argv[]) {

    if (argc != 4 && argc != 5) {
        fprintf(stdout, "Usage: %s <Causal> <ServerName> <ServerPort>\n", argv[0]);
        fprintf(stdout, "Usage: %s <Causal> <ServerName> <ServerPort> <command>\n", argv[0]);
        fprintf(stdout, "Found %d args\n", argc);
        exit(1);
    }

    SysConfig::Consistency = Utils::str2consistency(argv[1]); //causal 
    std::string serverName = argv[2];
    unsigned short serverPort = atoi(argv[3]);
    std::string cmd;
    if (argc == 5) {
        cmd = argv[4];
    }

    try {
        // connect to the server
        PublicTxClient client(serverName, serverPort);
        do {
            std::string input;

            if (argc == 5) {
                input = argv[4];
            } else {
                std::cout << ">"; // wait for user input
                std::getline(std::cin, input);
            }

            std::vector<string> splitedInput;
            boost::split(splitedInput, input, boost::is_any_of(" "));
            bool inputInvalid = true;

            if (splitedInput[0] == "Echo") {
                if (splitedInput.size() == 2) {
                    std::string input = splitedInput[1];
                    std::string output;
                    client.Echo(input, output);
                    cout << output << "\n";
                    inputInvalid = false;
                }
            }  else if (splitedInput[0] == "Start") {
#if defined(CURE) || defined(TUNABLE_CAUSAL)
                //Usage e.g. Start
                if (splitedInput.size() == 1) {
                    std::string stateStr;
                    bool r = client.MyStart();
                    if (r) {
                        cout << "[INFO]:Session started \n";
                    } else {
                        cout << "[INFO]:Failed to start session.\n";
                    }
                    inputInvalid = false;
                }
#endif
            } else if (splitedInput[0] == "Get") {
#ifdef CURE
                //Usage e.g. Get 1
                if (splitedInput.size() == 2) {
                    std::vector<string> keyList, valueList;
                    boost::split(keyList, splitedInput[1], boost::is_any_of(","));

                    bool r = client.MyGet(keyList, valueList);
                    if (r) {
                        cout << "Got: \n";
                        for (int i = 0; i < keyList.size(); i++) {
                            cout << keyList[i] << " = " << valueList[i] << "\n";
                        }
                    } else {
                        std::cout << "Get Operation failed.\n";
                    }
                    inputInvalid = false;
                }
#endif
            } else if (splitedInput[0] == "TcGet") {
#ifdef TUNABLE_CAUSAL
                //Usage e.g. TcGet 1 0
                if (splitedInput.size() == 3) {
                    std::vector<string> keyList, valueList;
                    boost::split(keyList, splitedInput[1], boost::is_any_of(","));
					int level = std::stoi(splitedInput[2]);

                    bool r = client.TcGet(keyList, valueList, level);
                    if (r) {
                        cout << "Got: \n";
                        for (int i = 0; i < keyList.size(); i++) {
                            cout << keyList[i] << " = " << valueList[i] << "\n";
                        }
                    } else {
                        std::cout << "Get Operation failed.\n";
                    }
                    inputInvalid = false;
                }
#endif
            } else if (splitedInput[0] == "Check") {
                //Usage e.g. Check 1 one
                if (splitedInput.size() == 3) {
                    std::vector<string> keyList, expectedValueList, valueList;
                    boost::split(keyList, splitedInput[1], boost::is_any_of(","));
                    boost::split(expectedValueList, splitedInput[2], boost::is_any_of(","));

                    bool r = client.MyGet(keyList, valueList);
                    if (r) {

                        cout << "Checking: \n";
                        for (int i = 0; i < keyList.size(); i++) {
                            if (expectedValueList[i] == valueList[i]) {
                                cout << "[OK] " << keyList[i] << " == " << valueList[i] << endl;
                            } else {
                                cout << "ERROR! Expected [" << expectedValueList[i] << "] but found [" << valueList[i]
                                     << "]" << endl;
                            }
                        }

                    } else {
                        cout << "Check operation failed.\n";
                    }
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "Put") {
#ifdef CURE
                //Usage e.g. Put 1 one
                if (splitedInput.size() == 3) {
                    std::vector<string> keyList, valueList;
                    boost::split(keyList, splitedInput[1], boost::is_any_of(","));
                    boost::split(valueList, splitedInput[2], boost::is_any_of(","));
                    for (int i = 0; i < keyList.size(); i++) {
                        cout << "Putting "<< keyList[i] << " = " << valueList[i] << endl;
                    }

                    bool r = client.MyPut(keyList, valueList);
                    if (r) {
                        cout << "Assigned: \n";
                        for (int i = 0; i < keyList.size(); i++) {
                            cout << keyList[i] << " = " << valueList[i] << endl;
                        }
                    } else {
                        cout << "Put operation failed.\n";
                    }
                    inputInvalid = false;
                }
#endif
            } else if (splitedInput[0] == "TcPut") {
#ifdef TUNABLE_CAUSAL
                //Usage e.g. TcPut 1 one MW
                if (splitedInput.size() == 4) {
                    std::vector<string> keyList, valueList;
                    boost::split(keyList, splitedInput[1], boost::is_any_of(","));
                    boost::split(valueList, splitedInput[2], boost::is_any_of(","));
					int level = std::stoi(splitedInput[3]);

                    for (int i = 0; i < keyList.size(); i++) {
                        cout << "Putting "<< keyList[i] << " = " << valueList[i] << endl;
                    }

                    bool r = client.TcPut(keyList, valueList, level);
                    if (r) {
                        cout << "Assigned: \n";
                        for (int i = 0; i < keyList.size(); i++) {
                            cout << keyList[i] << " = " << valueList[i] << endl;
                        }
                    } else {
                        cout << "Put operation failed.\n";
                    }
                    inputInvalid = false;
                }
#endif
            }

            if (inputInvalid) {
                std::cout << "Invalid input.\n";
                showUsage();
            }
        } while (argc == 4);

    } catch (SocketException &e) {
        fprintf(stdout, "SocketException: %s\n", e.what());
        exit(1);
    }

}

void showUsage() {
    std::string usage = "Echo <text>\n"
            "Get <key>\n"
            "Put <key> <value>\n"
            "Check <key> <value>\n"
			"TcGet <key> <level>\n"\
			"TcPut <key> <value> <level>\n";
    fprintf(stdout, "%s", usage.c_str());
}

