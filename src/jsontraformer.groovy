import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.apache.nifi.processor.io.StreamCallback

import java.nio.charset.StandardCharsets

def flowFile = session.get()
if(!flowFile) return

flowFile = session.write(flowFile, { inputStream, outputStream ->

    def content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    def jsonPayload = new JsonSlurper().parseText(content);
    String priorityList = jsonPayload.priorityList;
    def storePriority = jsonPayload.storePriority;

    if(StringUtils.isNoneEmpty(priorityList) && priorityList.contains("{{") && priorityList.contains("}}]"))
    {
        def trimmedObject = priorityList.substring(priorityList.indexOf("{{")+2, priorityList.indexOf("}}]"));


        List<String> values = trimmedObject.split(",\\s")

        values.each { String it ->
            try {
                def keyVal = it.split("=")
                def value = keyVal[1].toString()
                def key = keyVal[0].toString();

                if (value.contains(",")) {
                    value = value.split(",")
                }

                if (key.equalsIgnoreCase("Classification")) {
                    jsonPayload.productClassification = value;
                } else if (key.equalsIgnoreCase("Fullfillment")) {
                    jsonPayload.fullfillment = value;
                } else if (key.equalsIgnoreCase("returnStore") && value != '') {
                    List obj = new ArrayList();
                    storePriority.each { store ->
                        if (store.name.equalsIgnoreCase(value)) {
                            obj.add(store)
                        }
                    }
                    jsonPayload.returnStorePriority = obj;
                } else if (key.equalsIgnoreCase("newPincode")) {
                    jsonPayload.newPincode = value;
                } else if (key.equalsIgnoreCase("newCity")) {
                    jsonPayload.newCity = value;
                } else if (key.equalsIgnoreCase("sourcingChange")) {
                    jsonPayload.sourcingChange = value;
                } else if (key.equalsIgnoreCase("returnStoreChange")) {
                    jsonPayload.returnStoreChange = value;
                } else if (key.contains("storePriority")) {
                    storePriority.each { store ->
                        store.priority = 1000;
                        value.eachWithIndex { dcName, number ->
                            if (store.name.equalsIgnoreCase(dcName.toString())) {
                                store.priority = number
                            }
                        }
                    }
                }
                StringBuilder _id = new StringBuilder(jsonPayload.programId).append("_").append(jsonPayload.destnPincode).append("_").append(jsonPayload.productClassification)
                jsonPayload._id = _id.toString()
                jsonPayload.remove("priorityList")
                //objectMap.put(key, value);
            }
            catch (e) {
                log.error("Error during processing", e)
                //session.transfer(inputStream, REL_FAILURE)
            }
        }



        def outJson = new JsonBuilder(jsonPayload);
        outputStream.write(outJson.toString().getBytes(StandardCharsets.UTF_8))
    }

} as StreamCallback)
session.transfer(flowFile, REL_SUCCESS)

void updateRecord(Map<String, Object> record,String key, Object value){
    record.put(key, value);
}
