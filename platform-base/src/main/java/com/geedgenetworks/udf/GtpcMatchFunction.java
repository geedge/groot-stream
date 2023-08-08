package com.geedgenetworks.udf;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;

public class GtpcMatchFunction implements UdfFunction {


    private static final Log logger = LogFactory.get();
    private ProjectionFunction projection_function;
    public GtpcMatchFunction(ProjectionFunction projection_function) {
        this.projection_function=projection_function;
    }
/*    @Override
    public void udffunction(Map<String, Object> AuxiliaryFieldsAndValue) {


        try {
            String teid = null;
            String[] exprs = projection_function.getParameters().get("param").toString().split(",");
            for (String expr : exprs) {
                Object result = JsonPathUtil.analysis( AuxiliaryFieldsAndValue.get("tunnels").toString(), expr);
                if (result != null) {
                    teid = result.toString();
                    break;
                }
            }

            if (teid != null) {
                if (commonConfig.DEFAULT_RELATIONSHIP_MODULE.equals(commonConfig.DATA_RELATIONSHIP_MODEL)) {
                    int vsysId = Integer.parseInt(AuxiliaryFieldsAndValue.get("common_vsys_id").toString());
                    teid = teid + vsysId;
                }
                HashMap<String, Object> userData = HBaseUtils.getGtpData(teid);
                if (userData != null) {
                    for (String key : projection_function.getOutput_fields()) {
                        AuxiliaryFieldsAndValue.put(key, userData.get(key).toString());
                    }
                } else {
                    logger.warn("Description The user whose TEID is " + teid + " was not matched!");
                }
            }
        } catch (RuntimeException re) {
            logger.error("An exception occurred in teid type conversion or parsing of user information!" + re.getMessage());
            re.printStackTrace();
        }
    }*/

    @Override
    public void udffunction(Event event) {

    }
}
