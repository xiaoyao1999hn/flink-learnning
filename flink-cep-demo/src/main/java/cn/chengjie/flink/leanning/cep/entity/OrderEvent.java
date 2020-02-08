package cn.chengjie.flink.leanning.cep.entity;

import lombok.*;

import java.io.Serializable;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/17 15:28
 **/
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class OrderEvent  implements Serializable {

    private String userId;

    private String type;

}
