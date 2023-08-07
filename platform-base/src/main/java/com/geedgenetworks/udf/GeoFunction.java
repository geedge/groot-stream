package com.geedgenetworks.udf;

import com.geedgenetworks.pojo.GrootEvent;
import com.geedgenetworks.pojo.ProjectionFunction;

public class GeoFunction implements UdfFunction {

    private ProjectionFunction projection_function;
    public GeoFunction(ProjectionFunction projection_function) {
        this.projection_function=projection_function;
    }


/*    @Override
    public void udffunction(Map<String, Object> AuxiliaryFieldsAndValue) {



        String type =projection_function.getParameters().get("type").toString();
        switch(type) {
            case "asn":
                String asn = IpLookupUtils.getGeoAsn(String.valueOf(AuxiliaryFieldsAndValue.get(projection_function.getLookup_fields().get(0))));
                AuxiliaryFieldsAndValue.put(projection_function.getOutput_fields().get(0),asn);
                break;
            case "city":
                String city = IpLookupUtils.getGeoIpDetail(String.valueOf(AuxiliaryFieldsAndValue.get(projection_function.getLookup_fields().get(0))));
                AuxiliaryFieldsAndValue.put(projection_function.getOutput_fields().get(0),city);

                break;
            case "country":
                String country = IpLookupUtils.getGeoIpCountry(String.valueOf(AuxiliaryFieldsAndValue.get(projection_function.getLookup_fields().get(0))));
                AuxiliaryFieldsAndValue.put(projection_function.getOutput_fields().get(0),country);
                break;
        }


    }*/

    @Override
    public void udffunction(GrootEvent grootEvent) {

    }
}
