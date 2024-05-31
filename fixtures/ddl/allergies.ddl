(start DATE COMMENT 'The date the allergy was diagnosed.',
stop DATE COMMENT 'The date the allergy ended, if applicable.',
patient_id UUID COMMENT 'Foreign key to the Patient.',
encounter_id UUID COMMENT 'Foreign key to the Encounter when the allergy was diagnosed.',
code STRING COMMENT 'Allergy code',
system STRING COMMENT 'Terminology system of the Allergy code. RxNorm if this is a medication allergy, otherwise SNOMED-CT.',
description STRING COMMENT 'Description of the Allergy',
type STRING COMMENT 'Identify entry as an allergy or intolerance.',
category STRING COMMENT 'Identify the category as drug, medication, food, or environment.',
reaction1 STRING COMMENT 'Optional SNOMED code of the patients reaction.',
description1 STRING COMMENT 'Optional description of the Reaction1 SNOMED code.',
severity1 STRING COMMENT 'Severity of the reaction: MILD, MODERATE, or SEVERE.',
reaction2 STRING COMMENT 'Optional SNOMED code of the patients second reaction.',
description2 STRING COMMENT 'Optional description of the Reaction2 SNOMED code.',
severity2 STRING COMMENT 'Severity of the second reaction: MILD, MODERATE, or SEVERE.')