import csv
import sys
from elasticsearch import Elasticsearch


def upsert_doc_es(es, index_name, doc_type_name, file_path, identifier_col):
    with open(file_path) as f:
        csv_file_object = csv.reader(f, delimiter=',', quotechar='"')
        # reading the data from the file and capturing the information in the header to use when building our index
        header = csv_file_object.next()
        header = [item.lower() for item in header]
        bulk_data = []  # building up a Python dictionary of data set in a format that the Python ES client can use.
        # here we are loading data by means of bulk indexing
        for row in csv_file_object:
            data_dict = {}
            for i in range(len(row)):
                data_dict[header[i]] = row[i]
            op_dict = {
                "index": {
                    "_index": index_name,
                    "_type": doc_type_name,
                    "_id": data_dict[identifier_col]
                }
            }
            # bulk index request takes 2 lines for each operation, one is metadata and other is actual data.
            # op_dict is metadata
            bulk_data.append(op_dict)
            # data_dict is actual data
            bulk_data.append(data_dict)
    res = es.bulk(index=INDEX_NAME, body=bulk_data, refresh=True)


# create ES client, create index
def create_index(es, INDEX_NAME):
    if es.indices.exists(INDEX_NAME):
        print "Index already exists"
        print("deleting '%s' index..." % (INDEX_NAME))
        res = es.indices.delete(index=INDEX_NAME)
        print(" response: '%s'" % (res))
    # since we are running locally, use one shard and no replicas
    request_body = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }
    print("creating '%s' index..." % (INDEX_NAME))
    res = es.indices.create(index=INDEX_NAME, body=request_body)
    print(" response: '%s'" % (res))

    print "searching response"
    res = es.search(index=INDEX_NAME, size=10, body={"query": {"match_all": {}}})
    print(" response: '%s'" % (res))


def check_if_indices_exists(es, index_name):
    if es.indices.exists(index_name):
        print "Index exists"
        return True
    else:
        print "Index doesnt exists"
        return False


def search_from(es, index_name, searchFor, searchKey, size):
    return es.search(index=index_name, body={
        "size": size,
        "_source": ["name", "age"],
        'query': {
            'match': {
                searchFor: searchKey,
            }
        }
    })


def create_index_alias(es, index_name, alias_name):
    if check_if_indices_exists(es, index_name):
        alias_status = es.indices.put_alias(index=index_name, name=alias_name, body={
            "filter": {
                "match": {
                    "TId": alias_name
                }
            }
        })
        print "alias created", alias_status
    else:
        print "index doesn't exist, not creating alias"


if __name__ == "__main__":
    host = raw_input("Host for Elastic Search: ")
    port = raw_input("Port no for Elastic Search: ")
    ES_HOST = {"host": host, "port": port}
    # creating ES client
    es = Elasticsearch(hosts=[ES_HOST])
    print "Connected: " + str(es.ping())
    while True:
        print "1. Create index"
        print "2. Upsert data"
        print "3. Query"
        print "4. Exit"
        option = int(raw_input("Enter your option: "))
        if option == 1:
            INDEX_NAME = raw_input("Give index name: ")
            alias = raw_input("Give alias name: ")
            # create an index
            create_index(es, INDEX_NAME)
            create_index_alias(es, INDEX_NAME, alias)

        elif option == 2:
            INDEX_NAME = raw_input("Give index name: ")
            doc_type = raw_input("Give the document type name")
            file_path = raw_input("Give the filepath of the dataset")
            id = raw_input("Give the unique id of the dataset")
            upsert_doc_es(es, INDEX_NAME, doc_type, file_path, id)
            print("Successfully upserted your data...")
        elif option == 3:
            INDEX_NAME = raw_input("Give index name: ")
            field = raw_input("Give search field name to search:")
            search_key = raw_input("Give search key :")
            size = int(raw_input("Give result size:"))
            search_val = search_from(es, INDEX_NAME, field, search_key, size)
            for hit in search_val['hits']['hits']:
                print(hit['_source']['name'])
        elif option == 4:
            print("Bye! Bye! Exiting.......")
            sys.exit(0)
        else:
            print("Invalid option, please select correct option")
