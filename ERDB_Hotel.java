import com.datastax.driver.core.*;

import java.util.*;

import static com.sun.xml.internal.ws.spi.db.BindingContextFactory.LOGGER;
import static java.lang.System.out;

public class ERDB_Hotel {
    private Cluster cluster;
    private Session session;

    public void connect(final String node, final int port)
    {
        this.cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
        final Metadata metadata = cluster.getMetadata();
        out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        for (final Host host : metadata.getAllHosts())
        {
            out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }

    public Session getSession()
    {
        return this.session;
    }

    public void close()
    {
        cluster.close();
    }

    public void fetchSchemaData() {
        // fetching number of queries
        String countQueryStatement="select count(*) as count from erql.query";
        LOGGER.info(countQueryStatement);
        ResultSet resultSet=session.execute(countQueryStatement);
        Row x=resultSet.one();
        int count= (int) x.getLong("count");
        out.println(count);
        HashMap<String,HashSet<String>> schemaDetails=new HashMap<>();
        HashMap<String,HashMap<Integer, ArrayList<String>>> checkExistingSchema=new HashMap<>();
        HashMap<Integer,HashSet<String>> querySchemaDetails=new HashMap<>();
        for(int i=1;i<=count;i++){

            // fetching important parameters for schema

            String queryDetailsStatement="select get_entity,predicate_entity,equality_attributes,range_attributes,order_type from erql.query where query_no="+i;
            ResultSet resultSet1=session.execute(queryDetailsStatement);
            Row y=resultSet1.one();
            out.println("Details for query "+i);
            String entity_name=y.getString("get_entity");
            String predicate_table=y.getString("predicate_entity");
            Set<String> equality_attribute=y.getSet("equality_attributes",String.class);
            Set<String> range_attribute=y.getSet("range_attributes",String.class);
            Set<String> order_type= y.getSet("order_type",String.class);
            String table_name="";
            HashSet<String> attributesSet=new HashSet<>();
            String[] predicateEntityArray=predicate_table.split("[.]+");
            String lastPredicateEntity=predicateEntityArray[predicateEntityArray.length-1];
            int flag=0;
            HashMap<Integer,ArrayList<String>> existingQueryDetails=new HashMap<>();
            if(checkExistingSchema.containsKey(entity_name+"_by_"+predicate_table))
                flag=1;
            // logic to provide table name for each query

            if(predicate_table.contains(entity_name)  && !equality_attribute.isEmpty()){
                table_name=entity_name+"_by_"+equality_attribute.iterator().next();
            }
            else if(predicate_table.contains(entity_name)  && equality_attribute.isEmpty()){
                table_name=entity_name+"_by_"+range_attribute.iterator().next();
            }
            else if(schemaDetails.containsKey(entity_name+"_by_"+lastPredicateEntity) && !equality_attribute.isEmpty()){
                String equalityAttribute=equality_attribute.iterator().next();
                String entityStatement="select entity_name from erql.entity_attribute where attribute_name='"+equalityAttribute+"' ALLOW FILTERING";
                ResultSet resultSet7=session.execute(entityStatement);
                Row c=resultSet7.one();
                String attributeBasedEntity=c.getString("entity_name");
                if(attributeBasedEntity.equals(entity_name))
                    table_name=equalityAttribute+"_by_"+lastPredicateEntity;
                else
                    table_name=entity_name+"_by_"+equalityAttribute;
            }
            else if(schemaDetails.containsKey(entity_name+"_by_"+lastPredicateEntity) && equality_attribute.isEmpty()){
                String rangeAttribute=range_attribute.iterator().next();
                String entityStatement="select entity_name from erql.entity_attribute where attribute_name='"+rangeAttribute+"' ALLOW FILTERING";
                ResultSet resultSet7=session.execute(entityStatement);
                Row d=resultSet7.one();
                String attributeBasedEntity=d.getString("entity_name");
                if(attributeBasedEntity.equals(entity_name))
                    table_name=rangeAttribute+"_by_"+lastPredicateEntity;
                else
                    table_name=entity_name+"_by_"+rangeAttribute;
            }
            else
                table_name=entity_name+"_by_"+lastPredicateEntity;

            // Logic to fetch attributes required for schema based on query

            if(schemaDetails.containsKey(entity_name+"_by_"+predicate_table)){
                out.println("existing.....");
                attributesSet.addAll(schemaDetails.get(entity_name+"_by_"+predicate_table));
            }
            else{
                String schemaAttributesStatement="select attribute_name,attribute_type from erql.entity_attribute where entity_name='"+entity_name+"' ALLOW FILTERING";
                ResultSet resultSet2=session.execute(schemaAttributesStatement);
                for(Row z:resultSet2.all()) {
                    if(z.getString("attribute_type").equals("composite")){
                        String compositeAttributesStatement="select sub_attribute_name from erql.entity_composite_attribute where attribute_name='"+z.getString("attribute_name")+"' ALLOW FILTERING";
                        ResultSet resultSet6=session.execute(compositeAttributesStatement);
                        for(Row r:resultSet6.all())
                            attributesSet.add(r.getString("sub_attribute_name"));
                    }
                    else{
                        attributesSet.add(z.getString("attribute_name"));
                    }
                }
                if(predicate_table.contains(".")){
                    String[] predicateEntities=predicate_table.split("[.]+");
                    out.println(predicate_table);
                    out.println(predicateEntities.length);
                    for(int entity=0; entity<predicateEntities.length-1; entity++) {
                        out.println("inside attr......");
                        String primaryAttributesOfPredicateEntityStatement = "select primary_key from erql.entity where entity_name='" + predicateEntities[entity] + "'";
                        ResultSet resultSet4 = session.execute(primaryAttributesOfPredicateEntityStatement);
                        Row a = resultSet4.one();
                        String primaryKeyForPredicateEntity = a.getString("primary_key");
                        String[] primaryAttributes = primaryKeyForPredicateEntity.split(",");
                        for(String attribute: primaryAttributes)
                            attributesSet.add(attribute);
                    }
                }
                schemaDetails.put(entity_name+"_by_"+predicate_table,attributesSet);
            }

            // logic to fetch attributes required for partition key

            HashSet<String> partitionKeySet=new HashSet<>();
            if(!equality_attribute.isEmpty()){
                for(String partitionAttribute:equality_attribute){
                    if(attributesSet.contains(partitionAttribute)){
                        String multivalueAttributeStatement="select multivalue from erql.entity_attribute where attribute_name='"+partitionAttribute+"' allow filtering";
                        ResultSet resultSet3=session.execute(multivalueAttributeStatement);
                        Row b=resultSet3.one();
                        int isMultivalue=b.getInt("multivalue");
                        if(isMultivalue!=1)
                            attributesSet.remove(partitionAttribute);
                    }
                    partitionKeySet.add(partitionAttribute);
                }
            }
            else{
                String predicateEntity="";
                if(predicate_table.contains(".")){
                    String[] predicateEntities=predicate_table.split("[.]+");
//                    predicateEntity =predicateEntities[predicateEntities.length-1];
                    for(String entity:predicateEntities){
                        String primaryAttributesOfPredicateEntityStatement="select primary_key from erql.entity where entity_name='"+entity+"'";
                        ResultSet resultSet4=session.execute(primaryAttributesOfPredicateEntityStatement);
                        Row a=resultSet4.one();
                        String primaryKeyForPredicateEntity=a.getString("primary_key");
                        String[] primaryAttributes=primaryKeyForPredicateEntity.split(",");
                        for(String partitionAttribute:primaryAttributes){
                            if(attributesSet.contains(partitionAttribute))
                                attributesSet.remove(partitionAttribute);
                            partitionKeySet.add(partitionAttribute);
                        }
                    }
                }
                else
                    predicateEntity=predicate_table;
                String primaryAttributesOfPredicateEntityStatement="select primary_key from erql.entity where entity_name='"+predicateEntity+"'";
                ResultSet resultSet4=session.execute(primaryAttributesOfPredicateEntityStatement);
                Row a=resultSet4.one();
                String primaryKeyForPredicateEntity=a.getString("primary_key");
                String[] primaryAttributes=primaryKeyForPredicateEntity.split(",");
                for(String partitionAttribute:primaryAttributes){
                    if(attributesSet.contains(partitionAttribute))
                        attributesSet.remove(partitionAttribute);
                    partitionKeySet.add(partitionAttribute);
                }
            }

            // logic to fetch attributes required for clustering key

            HashSet<String> clusteringKeySet=new HashSet<>();
            Iterator<String> rangeAttributeIterator= range_attribute.iterator();
            Iterator<String> orderTypeIterator= order_type.iterator();
            while(rangeAttributeIterator.hasNext() && orderTypeIterator.hasNext()){
                String clusteringAttribute=rangeAttributeIterator.next();
                clusteringKeySet.add(clusteringAttribute + " " + orderTypeIterator.next().toUpperCase());
                if(attributesSet.contains(clusteringAttribute))
                    attributesSet.remove(clusteringAttribute);
            }
            String primaryAttributesStatement="select primary_key from erql.entity where entity_name='"+entity_name+"'";
            ResultSet resultSet5=session.execute(primaryAttributesStatement);
            Row a=resultSet5.one();
            String primaryKeyForEntity=a.getString("primary_key");
            String[] primaryAttributes=primaryKeyForEntity.split(",");
            for(String clusteringAttribute:primaryAttributes){
                if(!partitionKeySet.contains(clusteringAttribute))
                    clusteringKeySet.add(clusteringAttribute+" "+"ASC");
                if(attributesSet.contains(clusteringAttribute))
                    attributesSet.remove(clusteringAttribute);
            }

            // printing the schema required for query

            out.println("table name is: "+table_name);
            out.println();
//            out.println("list of attributes for query "+i+" is:");
//            for(String attribute:attributesSet)
//                out.println(attribute);
//            out.println();
//            out.println("list of partition key attributes for query "+i+" is:");
//            for(String partitionKey:partitionKeySet)
//                out.println(partitionKey);
//            out.println();
//            out.println("list of clustering key attributes for query "+i+" is:");
//            for(String clusteringKey:clusteringKeySet)
//                out.println(clusteringKey);
            out.println("Schema details for query "+i+" is:");
            ArrayList<String> primeAttributes=new ArrayList<>();
            primeAttributes.addAll(partitionKeySet);
            primeAttributes.addAll(clusteringKeySet);
            existingQueryDetails.put(i,primeAttributes);
            if(flag==1){
                out.println("In flag=1...............");
                HashMap<Integer,ArrayList<String>> tempMap=checkExistingSchema.get(entity_name+"_by_"+predicate_table);
                out.println(tempMap);
                ArrayList<String> tempArrList=new ArrayList<>();
                int queryNo=0;
                for(Map.Entry<Integer,ArrayList<String>> entry: tempMap.entrySet()) {
                    tempArrList=entry.getValue();
                    queryNo=entry.getKey();
                }
                if(tempArrList.equals(primeAttributes)){
                    out.println("Query no "+i+" can be answered using schema of query no "+ queryNo);
                    HashSet<String> schema=querySchemaDetails.get(queryNo);
                    for(String schemaAttributes:schema)
                        out.println(schemaAttributes);
                }
                else
                    flag=0;
            }

            if(flag==0){
                HashSet<String> finalSchema=new HashSet<>();
                for(String attributes:attributesSet)
                    finalSchema.add(attributes);
                for(String partitionKeyAttributes:partitionKeySet)
                    finalSchema.add(partitionKeyAttributes+" (K)");
                for(String clusteringKeyAttributes:clusteringKeySet)
                    finalSchema.add(clusteringKeyAttributes+" (C)");
                for(String schemaAttributes:finalSchema)
                    out.println(schemaAttributes);
                checkExistingSchema.put(entity_name+"_by_"+predicate_table,existingQueryDetails);
                querySchemaDetails.put(i,finalSchema);
            }
            out.println();
            out.println();
        }
    }

    public static void main(final String[] args)
    {
        final ERDB_Hotel client = new ERDB_Hotel();
        final String ipAddress = "localhost";
        final int port = 9042;
        out.println("Connecting to IP Address " + ipAddress + ":" + port + "...");
        client.connect(ipAddress, port);
        client.fetchSchemaData();
        client.close();
    }
}
