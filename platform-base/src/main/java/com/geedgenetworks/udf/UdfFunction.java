package com.geedgenetworks.udf;


import com.geedgenetworks.pojo.Event;

import java.io.Serializable;

public interface UdfFunction extends Serializable {


      void udffunction(Event event);


    }
