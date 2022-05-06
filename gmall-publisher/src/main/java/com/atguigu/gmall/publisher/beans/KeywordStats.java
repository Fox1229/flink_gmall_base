package com.atguigu.gmall.publisher.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author: Felix
 * Desc: 关键词统计实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordStats {

    private String stt;
    private String edt;
    private String keyword;
    private Long ct;
    private String ts;
}