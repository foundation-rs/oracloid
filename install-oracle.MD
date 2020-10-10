install clang

    sudo apt install clang -y

install libaio1

    sudo apt install libaio1 -y

copy oracle instant client include to /usr/include/oracle/instantclient_19_3/

go to instantclient/sdk/include

    sudo mkdir -p /usr/include/oracle/instantclient_19_3/
    sudo cp * -R /usr/include/oracle/instantclient_19_3/
    sudo chmod o+r -R /usr/include/oracle

copy oracle instant client libs to /opt/oracle/instantclient_19_3/

go to instantclient/sdk/include

    sudo mkdir -p /opt/oracle/instantclient_19_3/
    sudo cp -H -r *.so /opt/oracle/instantclient_19_3/
    sudo cp -r *.so.19.1 /opt/oracle/instantclient_19_3/

    libclntshcore.so.19.1 libclntsh.so.19.1 libmql1.so libipc1.so libnnz19.so libocci.so.19.1 liboramysql19.so libocijdbc19.so libociei.so

make links

    cd /opt/oracle/instantclient_19_3/
    ln -s libclntsh.so.19.1 libclntsh.so
    ln -s libclntshcore.so.19.1 libclntshcore.so

    sudo chmod o+r -R /opt/oracle/

add to .bashrc: export LD_LIBRARY_PATH="/opt/oracle/instantclient_19_3/"

add LD_LIBRARY_PATH="/opt/oracle/instantclient_19_3/" to project run enviornment

add oracle-instant-client.conf to /etc/ld.so.conf.d/

    sudo cp oracle-instant-client.conf /etc/ld.so.conf.d/
    sudo ldconfig
