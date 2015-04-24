/*
 * Copyright © 2013-2014 The Hyve B.V.
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

package org.transmartproject.db.dataquery.highdim.rnaseq

import org.transmartproject.core.dataquery.assay.Assay
import org.transmartproject.core.dataquery.highdim.chromoregion.Region
import org.transmartproject.db.dataquery.highdim.DeGplInfo
import org.transmartproject.db.dataquery.highdim.DeSubjectSampleMapping
import org.transmartproject.db.dataquery.highdim.chromoregion.DeChromosomalRegion
import org.transmartproject.db.i2b2data.PatientDimension
import org.transmartproject.db.querytool.QtQueryMaster

import static org.transmartproject.db.dataquery.highdim.HighDimTestData.*
import static org.transmartproject.db.querytool.QueryResultData.createQueryResult

class RnaSeqTestData {

    static final String TRIAL_NAME = 'REGION_SAMP_TRIAL_RNASEQ'

    static final String REGION_PLATFORM_MARKER_TYPE = 'RNASEQ_RCNT'

    DeGplInfo regionPlatform = {
        def p = new DeGplInfo(
                title: 'Test Region Platform',
                organism: 'Homo Sapiens',
                annotationDate: Date.parse('yyyy-MM-dd', '2013-05-03'),
                markerType: REGION_PLATFORM_MARKER_TYPE,
                genomeReleaseId: 'hg18',
        )
        p.id = 'test-region-platform_rnaseq'
        p
    }()

    DeGplInfo bogusTypePlatform = {
        def p = new DeGplInfo(
                markerTypeId: 'bogus marker type',
        )
        p.id = 'bogus-marker-platform_rnaseq'
        p
    }()

    List<DeChromosomalRegion> regions = {
        def r = [
                new DeChromosomalRegion(
                        platform: regionPlatform,
                        chromosome: '1',
                        start: 33,
                        end: 9999,
                        numberOfProbes: 42,
                        name: 'region 1:33-9999',
                ),
                new DeChromosomalRegion(
                        platform: regionPlatform,
                        chromosome: '2',
                        start: 66,
                        end: 99,
                        numberOfProbes: 2,
                        name: 'region 2:66-99',
                ),
        ]
        r[0].id = -1011L
        r[1].id = -1012L
        r
    }()

    List<PatientDimension> patients = createTestPatients(2, -2010, 'REGION_SAMP_TRIAL_RNASEQ')

    QtQueryMaster allPatientsQueryResult = createQueryResult(patients)

    List<DeSubjectSampleMapping> assays = createTestAssays(patients,
                                                           -3010L,
                                                           regionPlatform,
                                                           TRIAL_NAME)

    DeSubjectRnaseqData createRNASEQData(Region region,
                                     Assay assay,
                                     readcount = 0) {
        new DeSubjectRnaseqData(
                region:                     region,
                assay:                      assay,
                patient:                    assay.patient,
                readcount:                  readcount,
        )
    }

    List<DeSubjectRnaseqData> rnaseqData = {
        [
                createRNASEQData(regions[0], assays[0], -1),
                createRNASEQData(regions[0], assays[1], 0),
                createRNASEQData(regions[1], assays[0], 1),
                createRNASEQData(regions[1], assays[1], 2),
        ]
    }()

    void saveAll() {
        save([ regionPlatform, bogusTypePlatform ])
        save regions
        save patients
        save([ allPatientsQueryResult ])
        save assays
        save rnaseqData
    }

}
