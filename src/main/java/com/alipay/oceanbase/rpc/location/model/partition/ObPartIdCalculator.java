/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2021 OceanBase
 * %%
 * OBKV Table Client Framework is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

package com.alipay.oceanbase.rpc.location.model.partition;

import static com.alipay.oceanbase.rpc.location.model.partition.ObPartConstants.OB_PART_IDS_BITNUM;
import static com.alipay.oceanbase.rpc.location.model.partition.ObPartConstants.OB_PART_ID_SHIFT;

public class ObPartIdCalculator {
    public static final Long OB_TWOPART_BEGIN_MASK = (1L << OB_PART_IDS_BITNUM)
                                                     | 1L << (OB_PART_IDS_BITNUM + OB_PART_ID_SHIFT);

    /*
     * Generate part id.
     */
    public static Long generatePartId(Long partId1, Long partId2) {

        if (null == partId1) {
            return null;
        }

        if (null == partId2) {
            return partId1;
        }

        return (partId1 << OB_PART_ID_SHIFT) | partId2 | OB_TWOPART_BEGIN_MASK;
    }

    // get subpart_id with PARTITION_LEVEL_TWO_MASK
    public static long extractSubpartId(long id) {
        return id & (~(0xFFFFFFFFFFFFFFFFL << OB_PART_ID_SHIFT));
    }

    // get part_id with PARTITION_LEVEL_TWO_MASK
    public static long extractPartId(long id) {
        return id >> OB_PART_ID_SHIFT;
    }

    // get part idx from one level partid
    public static long extractIdxFromPartId(long id) {
        return id & (~(0xFFFFFFFFFFFFFFFFL << OB_PART_IDS_BITNUM));
    }

    // get part space from one level partid
    public static long extractSpaceFromPartId(long id) {
        return id >> OB_PART_IDS_BITNUM;
    }

    // get part_idx
    public static long extractPartIdx(long id) {
        return extractIdxFromPartId(extractPartId(id));
    }

    // get sub_part_idx
    public static long extractSubpartIdx(long id) {
        return extractIdxFromPartId(extractSubpartId(id));
    }
}
