#pylint: disable=C0111, C0301, C0325,I0011, W0403,I0011
from _communication import _Communication
from tools import _number_or_string, agent_name, group_address
import abcelogger
import csv
import db
import inspect
import jzmq as zmq
import time
import os
from repeat import repeat
import itertools
import postprocess
try:
    from interfaces import SimulationInterface                                                                                                                        #pylint: disable=F0401
except ImportError:
    class SimulationInterface: pass                                                                                                                                   #pylint: disable=C0321




class Simulation(SimulationInterface):
    """ This class in which the simulation is run. It takes
    the simulation_parameters to set up the simulation. Actions and agents have to be
    added. databases and resource declarations can be added. Then runs
    the simulation.

    Usually the parameters are specified in a tab separated csv file. The first
    line are column headers.

    Args::

     simulation_parameters: a dictionary with all parameters. "name" and
     "num_rounds" are mandatory.


    Example::
     for simulation_parameters in read_parameters('simulation_parameters.csv'):
        action_list = [
        ('household', 'recieve_connections'),
        ('household', 'offer_capital'),
        ('firm', 'buy_capital'),
        ('firm', 'production'),
        ('household', 'buy_product')
        'after_sales_before_consumption'
        ('household', 'consume')
        ]
        w = Simulation(simulation_parameters)
        w.add_action_list(action_list)
        w.build_agents(Firm, 'firm', 'num_firms')
        w.build_agents(Household, 'household', 'num_households')

        w.declare_round_endowment(resource='labor_endowment', productivity=1, product='labor')
        w.declare_round_endowment(resource='capital_endowment', productivity=1, product='capital')

        w.panel_data('firm', command='after_sales_before_consumption')

        w.run()
    """
    def __init__(self, simulation_parameters):
        self.simulation_parameters = simulation_parameters
        self._action_groups = {}
        self.agent_list = {}
        self._action_list = []
        self._resource_commands = {}
        self._perish_commands = {}
        self._aesof_commands = {}
        self._resource_command_group = {}
        self._db_commands = {}
        self.num_agents = 0
        self.num_agents_in_group = {}
        self._build_first_run = True
        self._agent_parameters = None
        self.database_name = 'database'

        from config import zmq_transport #pylint: disable=F0401
        if zmq_transport == 'inproc':
            self._addresses_bind = self._addresses_connect = {
                'type': 'inproc',
                'command_addresse': "inproc://commands",
                'ready': "inproc://ready",
                'frontend': "inproc://frontend",
                'backend': "inproc://backend",
                'group_backend': "inproc://group_backend",
                'database': "inproc://database",
                'logger': "inproc://logger"
            }
        elif zmq_transport == 'ipc':
            self._addresses_bind = self._addresses_connect = {
                'command_addresse': "ipc://commands.ipc",
                'ready': "ipc://ready.ipc",
                'frontend': "ipc://frontend.ipc",
                'backend': "ipc://backend.ipc",
                'group_backend': "ipc://group_backend",
                'database': "ipc://database.ipc",
                'logger': "ipc://logger.ipc"
            }
        elif zmq_transport == 'tcp':
            from config import config_tcp_bind, config_tcp_connect #pylint: disable=F0401
            self._addresses_bind = config_tcp_bind
            self._addresses_connect = config_tcp_connect
        else:
            from config import config_custom_bind, config_custom_connect  #pylint: disable=F0401
            self._addresses_bind = config_custom_bind
            self._addresses_connect = config_custom_connect
        time.sleep(1)
        self.zmq_context = zmq.MyContext()
        self.commands = self.zmq_context.socket(zmq.PUB)
        self.commands.bind(self._addresses_bind['command_addresse'])
        self.ready = self.zmq_context.socket(zmq.PULL)
        self.ready.bind(self._addresses_bind['ready'])
        self._communication = _Communication(self._addresses_bind, self._addresses_connect, self.zmq_context)
        self._communication.start()
        self.ready.recv()
        self.communication_channel = self.zmq_context.socket(zmq.PUSH)
        self.communication_channel.connect(self._addresses_connect['frontend'])
        self._register_action_groups()
        self._db = db.Database(simulation_parameters['_path'], self.database_name, self._addresses_bind['database'], self.zmq_context)
        self._logger = abcelogger.AbceLogger(simulation_parameters['_path'], 'logger', self._addresses_bind['logger'], self.zmq_context)
        self._db.start()
        self._logger.start()

        self.aesof = False
        self.round = 0
        self.trade_logging = 'individual'
        try:
            self.trade_logging = simulation_parameters['trade_logging'].lower()
        except KeyError:
            self.trade_logging = 'individual'
            print("'trade_logging' in simulation_parameters.csv not set"
                ", default to 'individual', possible values "
                "('group' (fast) or 'individual' (slow) or 'off')")
        if not(self.trade_logging in ['individual', 'group', 'off']):
            print(type(self.trade_logging), self.trade_logging, 'error')
            SystemExit("'trade_logging' in simulation_parameters.csv can be "
                        "'group' (fast) or 'individual' (slow) or 'off'"
                        ">" + self.trade_logging + "< not accepted")
        assert self.trade_logging in ['individual', 'group', 'off']
        time.sleep(1)

    def add_action_list(self, action_list):
        """ add an `action_list`, which is a list of either:

        - tuples of an goup_names and and action
        - a single command string for panel_data or follow_agent
        - [tuples of an agent name and and action, currently not unit tested!]


        Example::

         action_list = [
            repeat([
                    ('Firm', 'sell'),
                    ('Household', 'buy')
                ],
                repetitions=10
            ),
            ('Household_03', 'dance')
            'panel_data_end_of_round_befor consumption',
            ('Household', 'consume'),
            ]
         w.add_action_list(action_list)
        """
        self.action_list = action_list

    def add_action_list_from_file(self, parameter):
        """ The action list can also be declared in the simulation_parameters.csv
        file. Which allows you to run a batch of simulations with different
        orders. In simulation_parameters.csv there must be a column with which
        contains the a declaration of the action lists:

        +-------------+-------------+--------------------------------------------+-----------+
        | num_rounds  | num_agents  | action_list                                | endowment |
        +=============+=============+============================================+===========+
        | 1000,       | 10,         | [ ('firm', 'sell'), ('household', 'buy')], | (5,5)     |
        +-------------+-------------+--------------------------------------------+-----------+
        | 1000,       | 10,         | [ ('firm', 'buf'), ('household', 'sell')], | (5,5)     |
        +-------------+-------------+--------------------------------------------+-----------+
        | 1000,       | 10,         | [ ('firm', 'sell'),                        |           |
        |             |             | ('household', 'calculate_net_wealth'),     |           |
        |             |             | ('household', 'buy')],                     | (5,5)     |
        +-------------+-------------+--------------------------------------------+-----------+

        The command::

            self.add_action_list_from_file('parameters['action_list'])

        Args::

            parameter
                a string that contains the action_list. The string can be read
                from the simulation_parameters file: parameters['action_list'], where action_list
                is the column header in simulation_parameters.


        """
        self.add_action_list(eval(parameter))
        #TODO test

    def _register_action_groups(self):
        """ makes methods accessable for the action_list """
        reserved_words = ['build_agents', 'run', 'ask_agent',
                'ask_each_agent_in', 'register_action_groups']
        for method in inspect.getmembers(self):
            if (inspect.ismethod(method[1]) and method[0][0] != '_'
                    and method[0] not in reserved_words):
                self._action_groups[method[0]] = method[1]
        self._action_groups['_advance_round_agents'] = self._advance_round_agents

    def declare_round_endowment(self, resource, productivity, product, command='default_resource', group='all'):
        """ Every round the agent gets 'productivity' units of good 'product' for
        every 'resource' he possesses.

        By default the this happens at the beginning of the round. You can change this.
        Insert the command string you chose it self.action_list. One command can
        be associated with several resources.

        Round endowments can be goup specific, that means that when somebody except
        this group holds them they do not produce. The default is 'all'. Restricting
        this to a group could have small speed gains.
        """
        productivity = str(productivity)
        if command not in self._resource_commands:
            self._resource_commands[command] = []

        if command in self._resource_command_group:
            if self._resource_command_group[command] != group:
                raise SystemExit('Different groups assigned to the same command')
        else:
            self._resource_command_group[command] = group
        self._resource_commands[command].append([resource, productivity, product])

    def _make_resource_command(self, command):
        resources_in_this_command = self._resource_commands[command][:]
        group = self._resource_command_group[command]
        group_and_method = [group, '_produce_resource_rent_and_labor']

        def send_resource_command():
            for productivity in resources_in_this_command:
                self.commands.send_multipart(group_and_method + productivity)
        return send_resource_command
        #TODO could be made much faster by sending all resource simultaneously
        #as in _make_perish_command

    def declare_perishable(self, good, command='perish_at_the_round_end'):
        """ This good only lasts one round and then disappears. For example
        labor, if the labor is not used today today's labor is lost.
        In combination with resource this is useful to model labor or capital.

        In the example below a worker has an endowment of labor and capital.
        Every round he can sell his labor service and rent his capital. If
        he does not the labor service for this round and the rent is lost.

        Args::

         good: the good that perishes
         [command: In order to perish at another point in time you can choose
         a commmand and insert that command in the action list.

         Example::

             w.declare_round_endowment(resource='LAB_endowment', productivity=1000, product='LAB')
             w.declare_round_endowment(resource='CAP_endowment', productivity=1000, product='CAP')
             w.declare_perishable(good='LAB')
             w.declare_perishable(good='CAP')

        """
        if command not in self._perish_commands:
            self._perish_commands[command] = []
        self._perish_commands[command].append(good)

    def _make_perish_command(self, command):
        goods = self._perish_commands[command][:]

        def send_perish_command():
            self.commands.send_multipart(['all', '_perish'] + goods)
        return send_perish_command

    #TODO also for other variables
    def panel_data(self, group, variables='goods', typ='FLOAT', command='round_end'):
        """ Ponel_data writes variables of a group of agents into the database, by default
        the db write is at the end of the round. You can also specify a command
        and insert the command you choose in the action_list.
        If you choose a custom command, you can declare a method that
        returns the variable you want to track. This function in the class of the
        agent must have the same name as the command.

        You can use the same command for several groups, that report at the
        same time.


        Args:
            group:
                can be either a group or 'all' for all agents
            variables (optional):
                default='goods' monitors all the goods the agent owns
                you can insert any variable your agent possesses. For
                self.knows_latin you insert 'knows_latin'. If your agent
                has self.technology you can use 'technology['formula']'
                In this case you must set the type to CHAR(50) with the
                typ='CHAR(50)' parameter.
            typ:
                the type of the sql variable (FLOAT, INT, CHAR(length))
                command

        Example in start.py::

         w.panel_data(group='Firm', command='after_production')

         or

         w.panel_data(group=firm)

        Optional in the agent::

            class Firm(AgentEngine):

            ...
            def after_production(self):
                track = {}
                track['t'] = 'yes'
                for key in self.prices:
                    track['p_' + key] = self.prices[key]
                track.update(self.product[key])
                return track
        """
        if variables != 'goods':
            raise SystemExit('Not implemented')
        if command not in self._db_commands:
            self._db_commands[command] = []
        self._db_commands[command].append([group, variables, typ])
        self._db.add_panel(group, command)

    def _make_db_command(self, command):
        db_in_this_command = self._db_commands[command][:]

        def send_db_command():
            for db_good in db_in_this_command:
                self.commands.send_multipart([group_address(db_good[0]), '_db_panel', command])
                # self._add_agents_to_wait_for(self.num_agents_in_group[db_good[0]])
        return send_db_command

    def _process_action_list(self, action_list):
        processed_list = []
        for action in action_list:
            if type(action) is tuple:
                if action[0] not in self.num_agents_in_group.keys() + ['all']:
                    SystemExit('%s in (%s, %s) in the action_list is not a known agent' % (action[0], action[0], action[1]))
                action_name = '_' + action[0] + '|' + action[1]
                self._action_groups[action_name] = self._make_ask_each_agent_in(action)
                processed_list.append(action_name)
            elif isinstance(action, repeat):
                nested_action_list = self._process_action_list(action.action_list)
                for _ in range(action.repetitions):
                    processed_list.extend(nested_action_list)
            else:
                processed_list.append(action)
        return processed_list

    def run(self):
        """ This runs the simulation """
        if not(self.agent_list):
            raise SystemExit('No Agents Created')
        if not(self.action_list) and not(self._action_list):
            raise SystemExit('No action_list declared')
        if not(self._action_list):
            self._action_list = self._process_action_list(self.action_list)
        for command in self._db_commands:
            self._action_groups[command] = self._make_db_command(command)
            if command not in self._action_list:
                self._action_list.append(command)

        for command in self._resource_commands:
            self._action_groups[command] = self._make_resource_command(command)
            if command not in self._action_list:
                self._action_list.insert(0, command)

        for command in self._perish_commands:
            self._action_groups[command] = self._make_perish_command(command)
            if command not in self._action_list:
                self._action_list.append(command)

        if self.aesof:
            self._action_groups['aesof'] = self._make_aesof_command()
            if 'aesof' not in self._action_list:
                self._action_list.insert(0, 'aesof')

        self._action_list.append('_advance_round_agents')

        self._write_description_file()
        self._displaydescribtion()
        self._add_agents_to_wait_for(self.num_agents)
        self._wait_for_agents()
        start_time = time.time()

        for year in xrange(self.simulation_parameters['num_rounds']):
            print("\nRound" + str("%3d" % year))
            for action in self._action_list:
                self._action_groups[action]()
                self._wait_for_agents_than_signal_end_of_comm()
                self.commands.send_multipart(['all', '_clearing__end_of_subround'])

        print(str("%6.2f" % (time.time() - start_time)))
        for agent in list(itertools.chain(*self.agent_list.values())):
            self.commands.send_multipart([agent.name, "!", "die"])
        for agent in list(itertools.chain(*self.agent_list.values())):
            while agent.is_alive():
                time.sleep(0.1)
        self._end_Communication()
        database = self.zmq_context.socket(zmq.PUSH)
        database.connect(self._addresses_connect['database'])
        database.send('close')
        logger = self.zmq_context.socket(zmq.PUSH)
        logger.connect(self._addresses_connect['logger'])
        logger.send('close')
        while self._db.is_alive():
            time.sleep(0.05)
        while self._communication.is_alive():
            time.sleep(0.025)
        postprocess.to_r_and_csv(os.path.abspath(self.simulation_parameters['_path']), self.database_name)
        self.zmq_context.destroy()

    def _make_ask_each_agent_in(self, action):
        group_address_var = group_address(action[0])
        number = self.num_agents_in_group[action[0]]

        def ask_each_agent_with_address():
            self._add_agents_to_wait_for(number)
            self.commands.send_multipart([group_address_var, action[1]])
        return ask_each_agent_with_address

    def ask_each_agent_in(self, group_name, command):
        """ This is only relevant when you derive your custom world/swarm not
        in start.py
        applying a method to a group of agents group_name, method.

        Args::

         agent_group: using group_address('group_name', number)
         method: as string

        """
        self._add_agents_to_wait_for(self.num_agents_in_group[group_name])
        self.commands.send_multipart([group_address(group_name), command])

    def ask_agent(self, group, idn, command):
        """ This is only relevant when you derive your custom world/swarm not
        in start.py
        applying a method to a single agent

        Args::

         agent_name as string or using agent_name('group_name', number)
         method: as string
        """
        self._add_agents_to_wait_for(1)
        self.commands.send_multipart(['%s_%i:' % (group, idn), command])

    def build_agents(self, AgentClass,  number=None, group_name=None, agents_parameters=None):                                                                  #pylint: disable=C0103
        """ This method creates agents, the first parameter is the agent class.
        "num_agent_class" (e.G. "num_firm") should be difined in
        simulation_parameters.csv. Alternatively you can also specify number = 1.s

        Args::

         AgentClass: is the name of the AgentClass that you imported
         number (optional): number of agents to be created. or the colum name
         of the row in simulation_parameters.csv that contains this number. If not
         specified the column name is assumed to be 'num_' + agent_name
         (all lowercase). For example num_firm, if the class is called
         Firm or name = Firm.
         [group_name (optional): to give the group a different name than the
         class_name. (do not use this if you have not a specific reason]

        Example::

         w.build_agents(Firm, number='num_firms')
         # 'num_firms' is a column in simulation_parameters.csv
         w.build_agents(Bank, 1)
         w.build_agents(CentralBank, number=1)
        """
        #TODO single agent groups get extra name without number
        #TODO when there is a group with a single agent the ask_agent has a confusingname
        if not(group_name):
            group_name = AgentClass.__name__.lower()
        if number and not(agents_parameters):
            try:
                num_agents_this_group = int(number)
            except ValueError:
                try:
                    num_agents_this_group = self.simulation_parameters[number]
                except KeyError:
                    SystemExit('build_agents ' + group_name + ': ' + number +
                    ' is not a number or a column name in simulation_parameters.csv'
                    'or the parameterfile you choose')
        elif not(number) and not(agents_parameters):
            try:
                num_agents_this_group = self.simulation_parameters['num_' + group_name.lower()]
            except KeyError:
                raise SystemExit('num_' + group_name.lower() + ' is not in simulation_parameters.csv')
        elif not(number) and agents_parameters:
            num_agents_this_group = len(agents_parameters)
            self.simulation_parameters['num_' + group_name.lower()] = num_agents_this_group
        else:
            raise SystemExit('build_agents ' + group_name + ': Either '
                'number_or_parameter_column or agents_parameters must be'
                'specied, NOT both.')
        if not(agents_parameters):
            agents_parameters = [None for _ in range(num_agents_this_group)]

        self.num_agents += num_agents_this_group
        self.num_agents_in_group[group_name] = num_agents_this_group
        self.num_agents_in_group['all'] = self.num_agents
        self.agent_list[group_name] = []
        for idn in range(num_agents_this_group):
            agent = AgentClass(self.simulation_parameters, agents_parameters[idn], [idn, group_name, self._addresses_connect, self.trade_logging, self.zmq_context])
            agent.name = agent_name(group_name, idn)
            agent.start()
            self.agent_list[group_name].append(agent)

    def build_agents_from_file(self, AgentClass, parameters_file=None, multiply=1, delimiter='\t', quotechar='"'):
        """ This command builds agents of the class AgentClass from an csv file.
        This way you can build agents and give every single one different
        parameters.

        The file must be tab separated. The first line contains the column
        headers. The first column "agent_class" specifies the agent_class. The
        second column "number" (optional) allows you to create more than one
        agent of this type. The other columns are parameters that you can
        access in own_parameters the __init__ function of the agent.

        Agent created from a csv-file::

         class Agent(AgentEngine):
            def __init__(self, simulation_parameter, own_parameters, _pass_to_engine):
                AgentEngine.__init__(self, *_pass_to_engine)
                self.size = own_parameters['firm_size']
        """
        #TODO declare all self.simulation_parameters['num_XXXXX'], when this is called the first time
        if parameters_file == None:
            try:
                parameters_file = self.simulation_parameters['agent_parameters_file']
            except KeyError:
                parameters_file = 'agents_parameters.csv'
        elif self._agent_parameters == None:
            if parameters_file != self._agent_parameters:
                SystemExit('All agents must be declared in the same agent_parameters.csv file')
        self._agent_parameters = parameters_file

        agent_class = AgentClass.__name__.lower()
        agents_parameters = []
        csvfile = open(parameters_file)
        dialect = csv.Sniffer().sniff(csvfile.read(1024))
        csvfile.seek(0)
        agent_file = csv.reader(csvfile, dialect)
        keys = [key for key in agent_file.next()]
        if not(set(('agent_class', 'number')).issubset(keys)):
            SystemExit(parameters_file + " does not have a column 'agent_class'"
                "and/or 'number'")

        agents_list = []
        for line in agent_file:
            cells = [_number_or_string(cell) for cell in line]
            agents_list.append(dict(zip(keys, cells)))

        if self._build_first_run:
            for line in agents_list:
                num_entry = 'num_' + line['agent_class'].lower()
                if num_entry not in self.simulation_parameters:
                    self.simulation_parameters[num_entry] = 0
                self.simulation_parameters[num_entry] += int(line['number'])
            self._build_first_run = False

        for line in agents_list:
            if line['agent_class'] == agent_class:
                agents_parameters.extend([line for _ in range(line['number'] * multiply)])

        self.build_agents(AgentClass, agents_parameters=agents_parameters)

    def debug_subround(self):
        self.subround = subround.Subround(self._addresses_connect)
        self.subround.name = "debug_subround"
        self.subround.start()

    def _advance_round_agents(self):
        """ advances round by 1 """
        self.round += 1
        self.commands.send_multipart(['all', '_advance_round'])

    def _add_agents_to_wait_for(self, number):
        self.communication_channel.send_multipart(['!', '+', str(number)])

    def _wait_for_agents_than_signal_end_of_comm(self):
        self.communication_channel.send_multipart(['!', '}'])
        try:
            self.ready.recv()
        except KeyboardInterrupt:
            print('KeyboardInterrupt: abce.db: _wait_for_agents_than_signal_end_of_comm(self) ~654')

    def _wait_for_agents(self):
        self.communication_channel.send_multipart(['!', ')'])
        try:
            self.ready.recv()
        except KeyboardInterrupt:
            print('KeyboardInterrupt: abce.db: _wait_for_agents(self) ~662')

    def _end_Communication(self):
        self.communication_channel.send_multipart(['!', '!', 'end_simulation'])

    def _write_description_file(self):
        description = open(
                os.path.abspath(self.simulation_parameters['_path'] + '/description.txt'), 'w')
        description.write('\n')
        description.write('\n')
        for key in self.simulation_parameters:
            description.write(key + ": " + str(self.simulation_parameters[key]) + '\n')

    def _displaydescribtion(self):
        description = open(self.simulation_parameters['_path'] + '/description.txt', 'r')
        print(description.read())

    def declare_aesof(self, aesof_file='aesof.csv'):
        """ AESOF lets you determine agents behaviour from an comma sepertated sheet.

        First row must be column headers. There must be one column header 'round' and
        a column header name. A name can be a goup are and individal (goup_id
        e.G. firm_01) it can also be 'all' for all agents.
        Every round, the agents self.aesof parameters get updated, if a row with
        the corresponding round and agent name exists.

        Therefore an agent can access the parameters `self.aesof[column_name]` for
        the current round. (or the precedent one when there was no update)
        parameter is set. You can use it in your source code. It is persistent
        until the next round for which a corresponding row exists.

        You can alse put commands or call methods in the excel file. For example:
         `self.aesof_exec(column_name)`.
        Alternatively you can declare a variable according to a
        function: `willingness_to_pay = self.aesof_eval(column_name)`.

        There is a big difference between `self.aesof_exec` and `self.aesof_eval`.
        exec is only executed in rounds that have corresponding rows in aesof.csv.
        `self.aesof_eval` is persistent every round the expression of the row
        corresponding to the current round round or the last declared round is
        executed.

        Args:
            aesof_file(optional):
                name of the csv_file. Default is the group name plus 'aesof.csv'.
        """
        csvfile = open(aesof_file)
        dialect = csv.Sniffer().sniff(csvfile.read(1024))
        csvfile.seek(0)
        reader = csv.reader(csvfile, dialect)
        keys = [key.lower() for key in reader.next()]
        if not 'name' in keys:
            SystemExit("no column 'name' in the aesof.csv")
        if not 'round' in keys:
            SystemExit("no column 'round' in the aesof.csv")
        self.aesof_dict = {}
        for line in reader:
            line = [_number_or_string(cell) for cell in line]
            dictionary = dict(zip(keys, line))
            round = int(dictionary['round'])
            if round not in self.aesof_dict:
                self.aesof_dict[round] = []
            self.aesof_dict[round].append(dictionary)
        if 0 not in self.aesof_dict:
            SystemExit('Round 0 must always be declare, with initial values')
        self.aesof = True

    def _make_aesof_command(self):
        for round in self.aesof_dict.keys():
            for round_line in self.aesof_dict[round]:
                if round_line['name'] not in self.num_agents_in_group.keys() + ['all']:
                    SystemExit("%s in '%s' under 'name' is an unknown agent" % (round_line['name'], round_line))

        def send_aesof_command():
            try:
                for round_line in self.aesof_dict[self.round]:
                    self.commands.send_multipart(['%s:' % round_line['name'], '_aesof'], zmq.SNDMORE)
                    self.commands.send_json(round_line)
            except KeyError:
                if self.round in self.aesof_dict:
                    raise
        return send_aesof_command

    #pylint: disable=C0103
    def getZmqContext(self):
        return self.zmq_context

    def getAddressesConnect(self):
        return self._addresses_connect

