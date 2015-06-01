# -----------------------------------
# Maintain the last-used-IDs for various tables
# Persisted to flat file
# -----------------------------------
class Table_ID_Values(object):
    def __init__(self):
        self.last_condition_occurrence_id = 1
        self.last_device_cost_id = 1
        self.last_device_exposure_id = 1
        self.last_drug_cost_id = 1
        self.last_drug_exposure_id = 1

        self.last_location_id = 1
        self.last_measurement_id = 1
        self.last_observation_id = 1
        self.last_observation_period_id = 1
        self.last_payer_plan_period_id = 1

        self.last_person_id = 1
        self.last_procedure_cost_id = 1
        self.last_procedure_occurrence_id = 1
        self.last_specimen_id = 1
        self.last_visit_cost_id = 1

        self.last_visit_occurrence_id = 1

        # care-site and provider are handled separately

    def Load(self, filename, log_stats):
        with open(filename,'r') as f_in:
            line = f_in.readline()
            flds = line.split()
            if len(flds) == 16:
                self.last_condition_occurrence_id = int(flds[0])
                self.last_device_cost_id = int(flds[1])
                self.last_device_exposure_id = int(flds[2])
                self.last_drug_cost_id = int(flds[3])
                self.last_drug_exposure_id = int(flds[4])

                self.last_location_id = int(flds[5])
                self.last_measurement_id = int(flds[6])
                self.last_observation_id = int(flds[7])
                self.last_observation_period_id = int(flds[8])
                self.last_payer_plan_period_id = int(flds[9])

                self.last_person_id = int(flds[10])
                self.last_procedure_cost_id = int(flds[10])
                self.last_procedure_occurrence_id = int(flds[12])
                self.last_specimen_id = int(flds[13])
                self.last_visit_cost_id = int(flds[14])

                self.last_visit_occurrence_id = int(flds[15])

        log_stats('--Table_ID_Values loaded:')
        log_stats('\tlast_condition_occurrence_id \t: {0}'.format(self.last_condition_occurrence_id))
        log_stats('\tlast_device_cost_id \t: {0}'.format(self.last_device_cost_id))
        log_stats('\tlast_device_exposure_id \t: {0}'.format(self.last_device_exposure_id))
        log_stats('\tlast_drug_cost_id \t: {0}'.format(self.last_drug_cost_id))
        log_stats('\tlast_drug_exposure_id \t: {0}'.format(self.last_drug_exposure_id))

        log_stats('\tlast_location_id \t: {0}'.format(self.last_location_id))
        log_stats('\tlast_measurement_id \t: {0}'.format(self.last_measurement_id))
        log_stats('\tlast_observation_id \t: {0}'.format(self.last_observation_id))
        log_stats('\tlast_observation_period_id \t: {0}'.format(self.last_observation_period_id))
        log_stats('\tlast_payer_plan_period_id \t: {0}'.format(self.last_payer_plan_period_id))

        log_stats('\tlast_person_id \t: {0}'.format(self.last_person_id))
        log_stats('\tlast_procedure_cost_id \t: {0}'.format(self.last_procedure_cost_id))
        log_stats('\tlast_procedure_occurrence_id \t: {0}'.format(self.last_procedure_occurrence_id))
        log_stats('\tlast_specimen_id \t: {0}'.format(self.last_specimen_id))
        log_stats('\tlast_visit_cost_id \t: {0}'.format(self.last_visit_cost_id))

        log_stats('\tlast_visit_occurrence_id \t: {0}'.format(self.last_visit_occurrence_id))

    def Save(self, filename):
        with open(filename,'w') as f_out:

            f_out.write('{0}\t'.format(self.last_condition_occurrence_id))
            f_out.write('{0}\t'.format(self.last_device_cost_id))
            f_out.write('{0}\t'.format(self.last_device_exposure_id))
            f_out.write('{0}\t'.format(self.last_drug_cost_id))
            f_out.write('{0}\t'.format(self.last_drug_exposure_id))

            f_out.write('{0}\t'.format(self.last_location_id))
            f_out.write('{0}\t'.format(self.last_measurement_id))
            f_out.write('{0}\t'.format(self.last_observation_id))
            f_out.write('{0}\t'.format(self.last_observation_period_id))
            f_out.write('{0}\t'.format(self.last_payer_plan_period_id))

            f_out.write('{0}\t'.format(self.last_person_id))
            f_out.write('{0}\t'.format(self.last_procedure_cost_id))
            f_out.write('{0}\t'.format(self.last_procedure_occurrence_id))
            f_out.write('{0}\t'.format(self.last_specimen_id))
            f_out.write('{0}\t'.format(self.last_visit_cost_id))

            f_out.write('{0}'.format(self.last_visit_occurrence_id))
            f_out.write('\n')


