import groovy.json.JsonSlurper

import javax.xml.crypto.dsig.keyinfo.KeyValue


def priorityList = "MapRecord[Document{{_id=5fe654b0351e0b753e137884, Pincode=733211, City=DALKOLA, State=WEST BENGAL, Fullfillment=HDEL, Classification=SA, storePriority=E017,E046,E002,E074, returnStore=E017, returnStoreCity=Kolkata, returnStoreState=West Bengal, newPincode=N, newCity=N, sourcingChange=Delete, returnStoreChange=Delete}}]"


def trimmedObject = priorityList.substring(priorityList.indexOf("{{")+2, priorityList.indexOf("}}]"));
//priorityList.replaceAll("\r?\n", "")
List<String> values = trimmedObject.split(",\\s")

Map<String, Object> objectMap = new TreeMap<>();
values.each {String it ->
    def keyVal = it.split("=")
    //StringBuilder builder = new StringBuilder(keyVal[0]).append(":").append(keyVal[1])
    def value = keyVal[1].toString()
    def key = keyVal[0].toString();

    if(value.contains(","))
    {
        value = value.split(",")
    }
    objectMap.put(key, value);
}

println(objectMap.toString())

/*priorityList.replace("\\=", '":"')
//def trimmedObject = priorityList.substring(priorityList.indexOf("[")+1, priorityList.indexOf("]"));
println(priorityList)*/
/*def priorityListJson = new JsonSlurper().parseText(priorityList);
def productClassification = priorityListJson.Classification;
println(productClassification.toString())
def priority = priorityListJson.priority
println(priority.toString())*/


