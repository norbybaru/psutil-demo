
import os
import psutil

class Monitoring:

    def get_running_process(self):
        return psutil.Process().as_dict(attrs=['pid', 'name', 'username'])

    def get_cpu_usage_pct(self):
        """
        Obtains the system's average CPU load as measured over a period of 500 milliseconds.
        :returns: System CPU load as a percentage.
        :rtype: float
        """
        return psutil.cpu_percent(interval=0.5)

    def get_cpu_frequency(self):
        """
        Obtains the real-time value of the current CPU frequency.
        :returns: Current CPU frequency in MHz.
        :rtype: int
        """
        return int(psutil.cpu_freq().current)

    def get_cpu_temp(self):
        """
        Obtains the current value of the CPU temperature.
        :returns: Current value of the CPU temperature if successful, zero value otherwise.
        :rtype: float
        """
        # Initialize the result.
        result = 0.0
        # The first line in this file holds the CPU temperature as an integer times 1000.
        # Read the first line and remove the newline character at the end of the string.
        if os.path.isfile('/sys/class/thermal/thermal_zone0/temp'):
            with open('/sys/class/thermal/thermal_zone0/temp') as f:
                line = f.readline().strip()
            # Test if the string is an integer as expected.
            if line.isdigit():
                # Convert the string with the CPU temperature to a float in degrees Celsius.
                result = float(line) / 1000
        # Give the result back to the caller.
        return result

    def get_ram_usage(self):
        """
        Obtains the absolute number of RAM bytes currently in use by the system.
        :returns: System RAM usage in bytes.
        :rtype: int
        """
        return int(psutil.virtual_memory().total - psutil.virtual_memory().available)


    def get_ram_total(self):
        """
        Obtains the total amount of RAM in bytes available to the system.
        :returns: Total system RAM in bytes.
        :rtype: int
        """
        return int(psutil.virtual_memory().total)


    def get_ram_usage_pct(self):
        """
        Obtains the system's current RAM usage.
        :returns: System RAM usage as a percentage.
        :rtype: float
        """
        return psutil.virtual_memory().percent

    def get_write_records(self):
        cpu_usage = {
            'MeasureName': 'cpu_usage',
            'MeasureValue': str(self.get_cpu_usage_pct())
        }

        cpu_frequency = {
            'MeasureName': 'cpu_freq',
            'MeasureValueType': 'BIGINT',
            'MeasureValue': str(self.get_cpu_frequency())
        }

        cpu_temperature = {
            'MeasureName': 'cpu_temp',
            'MeasureValueType': 'DOUBLE',
            'MeasureValue': str(self.get_cpu_temp())
        }

        ram_usage = {
            'MeasureName': 'ram_usage',
            'MeasureValue': str(int(self.get_ram_usage() / 1024 / 1024))
        }

        ram_total = {
            'MeasureName': 'ram_total',
            'MeasureValue': str(int(self.get_ram_total() / 1024 / 1024))
        }

        return [cpu_usage, cpu_frequency, cpu_temperature, ram_usage, ram_total]