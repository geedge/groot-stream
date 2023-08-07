package com.geedgenetworks.udf;


import com.geedgenetworks.pojo.GrootEvent;

import java.io.Serializable;

public interface UdfFunction extends Serializable {


      void udffunction(GrootEvent grootEvent);


    }
