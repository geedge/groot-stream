package net.geedgenetworks.udf;


import net.geedgenetworks.pojo.GrootEvent;

import java.io.Serializable;

public interface UdfFunction extends Serializable {


      void udffunction(GrootEvent grootEvent);


    }
