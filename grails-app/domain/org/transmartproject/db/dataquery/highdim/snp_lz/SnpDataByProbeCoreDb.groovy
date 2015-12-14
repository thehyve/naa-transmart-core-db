/*
 * Copyright © 2013-2015 The Hyve B.V.
 *
 * This file is part of transmart-core-db.
 *
 * Transmart-core-db is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * transmart-core-db.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.transmartproject.db.dataquery.highdim.snp_lz


import java.sql.Blob
import java.sql.Clob

class SnpDataByProbeCoreDb {

    String trialName
    String a1
    String a2
    Clob a1Clob
    Clob a2Clob
    BigDecimal imputeQuality
    Blob gpsByProbeBlob
    Blob gtsByProbeBlob
    Blob doseByProbeBlob
    BigDecimal gtProbabilityThreshold
    BigDecimal maf
    String minorAllele
    Long CA1A1
    Long CA1A2
    Long CA2A2
    Long CNocall

    /* other columns are ignored */

    String createdBy
    String modifiedBy
    Date dateCreated
    Date lastUpdated

    // unmapped relationship; see below
    // note however that this *will* create a double foreign key
    // if you configure hibernate to update the schema
    DeRcSnpInfo jRcSnpInfo


    static belongsTo = [
            snpInfo:                 DeSnpInfo,
            bioAssayGenoPlatform:    BioAssayGenoPlatformProbe,
            genotypeProbeAnnotation: GenotypeProbeAnnotation,
    ]

    static mapping = {
        table        schema:  'deapp', name: 'de_snp_data_by_probe'

        id                      column: 'snp_data_by_probe_id',          generator: 'assigned'
        bioAssayGenoPlatform    column: 'bio_asy_geno_platform_probe_id'
        genotypeProbeAnnotation column: 'genotype_probe_annotation_id'
        dateCreated             column: 'created_date'
        lastUpdated             column: 'modified_date'
        a1Clob                  column: 'a1_clob'
        a2Clob                  column: 'a2_clob'
        'CA1A1'                 column: 'c_a1_a1'
        'CA1A2'                 column: 'c_a1_a2'
        'CA2A2'                 column: 'c_a2_a2'
        'CNocall'               column: 'c_nocall'

        snpInfo                 column: 'snp_id'
        jRcSnpInfo              column: 'snp_id', insertable: false, updateable: false

        version false
    }

    static constraints = {
        trialName               nullable: true
        bioAssayGenoPlatform    nullable: true
        genotypeProbeAnnotation nullable: true
        a1                      nullable: true, maxSize: 4000
        a2                      nullable: true, maxSize: 4000
        a1Clob                  nullable: true
        a2Clob                  nullable: true
        imputeQuality           nullable: true, scale:   17
        gpsByProbeBlob          nullable: true
        gtsByProbeBlob          nullable: true
        doseByProbeBlob         nullable: true
        gtProbabilityThreshold  nullable: true, scale:   17
        maf                     nullable: true, scale:   17
        minorAllele             nullable: true, maxSize: 2
        'CA1A1'                 nullable: true
        'CA1A2'                 nullable: true
        'CA2A2'                 nullable: true
        'CNocall'               nullable: true

        createdBy               nullable: true, maxSize: 30
        dateCreated             nullable: true
        modifiedBy              nullable: true, maxSize: 30
        lastUpdated             nullable: true

        snpInfo                 nullable: true
        jRcSnpInfo              nullable: true
    }
}
