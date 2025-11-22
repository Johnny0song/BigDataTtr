package com.atguigu.spark.sparkcore.rdd.beans;




import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.AllArgsConstructor;

import java.io.Serializable;


@Data
@AllArgsConstructor
public class User {
    private String name;
    private Integer age;


}
