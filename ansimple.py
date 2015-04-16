#!/usr/bin/python
# -*- coding: utf-8 -*-
# 
# Ansible Runner made easy:
# Create your playbooks in Python
#
# Author: Pau Sanchez
#
# The MIT License (MIT)
#
# Copyright (c) 2015 Pau Sanchez
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
import os
import sys
import copy
import collections
import ansible.runner
import ansible.inventory
import ansible.cache
import ansible.utils
import ansible.constants as ansible_C
import json
import yaml

# modules that allow first parameter to be free style
FREE_STYLE_MODULES = set(['command', 'shell', 'script'])

class ansimple:
  """ Simple class to run ansible modules remotely, keep facts about each
  remote host, and keep variables.

  Example:
    >>> ans = ansimple ('inventory-file')
    >>> r = ans.ping()
    >>> r.all_ok
    True
    >>> ans.filter('db-servers').apt ('mysql-server', state='present')
    >>> ans.filter('web-servers').apt ('apache2', state='present')
    >>> ans.filter('wserver03').file ('/tmp/IAmServer03', state='touch')

  """
  def __init__ (self, inventory, pattern = 'all', default_vars = None):
    """
    default_vars sets the default variables for all hosts
    """
    if isinstance (inventory, ansible.inventory.Inventory):
      self.inventory = inventory

    elif isinstance (inventory, basestring):
      inventory = self._searchPath(inventory)
      self.inventory = ansible.inventory.Inventory (inventory)

      # fix key paths
      for host in self.inventory.get_hosts():
        pkf = host.vars.get ('ansible_ssh_private_key_file', None)
        if pkf:
          host.vars['ansible_ssh_private_key_file'] = self._searchPath(pkf)

    self.pattern = pattern
    self.private_key_file = None
    self.facts = collections.defaultdict(dict)
    self.vars = collections.defaultdict(dict)
    self.verbose = True

    # for each host, initialize 'ansible_hostname'
    for host in self.hosts():
      self.facts[host]['ansible_hostname'] = host

    # initialize variables for all hosts  
    if default_vars:  
      self.initvars ({'all' : copy.deepcopy(default_vars) })
    return

  def _searchPath(self, fname):
    """ Search file using current location and top directories
    """
    abspath = os.path.abspath (fname)
    if os.path.exists (abspath):
      return abspath

    # try to find fname
    root = os.path.abspath(os.path.dirname(__file__))
    while True:
      fpath = os.path.abspath(os.path.join (root, fname))
      if os.path.exists (fpath):
        return fpath
      new_root = os.path.dirname (root)
      if root == new_root:
        break
      root = new_root

    return None

  def task (self, name):
    if self.verbose:
      sys.stderr.write ('TASK[%s]: %s\n' % (self.pattern, name))
      sys.stderr.flush ()
    return

  def setPrivateKeyFile (self, private_key_file):
    self.private_key_file = os.path.abspath (private_key_file)
    return

  def filter (self, pattern):
    """ Filter to which servers we want to apply our actions to but sharing
    the same facts
    """
    filtered = ansimple (self.inventory, '%s:&%s' % (self.pattern, pattern))
    filtered.facts = self.facts
    filtered.vars  = self.vars
    return filtered

  @staticmethod
  def setHostKeyChecking (value):
    """ Overrides the default Host Key Checks when connecting to hosts
    """
    old = ansible_C.HOST_KEY_CHECKING
    ansible_C.HOST_KEY_CHECKING = value
    return old

  def __getattr__ (self, name):
    """ Makes possible to call a module transparently
    """
    return self.module_functor (name)

  def __getitem__ (self, host):
    return self.facts[host]

  def initvars (self, allvars):
    """ Initialize variables from given allvars map, or allvars file.

    In case allvars is a string, it will be interpreted as a file, and depending
    on the extension ('yml', 'yaml' or 'json') it will be loaded.

    The map should associate a set of name and value pairs to a pattern.
    Patterns will be the map indexes whereas the name, value pairs will be a
    map inside. The pattern 'all' will match all hosts, and will be the first one
    applied for the default values of all hosts.

    Then, the last patterns applied will be the ones for the hosts. Every other
    pattern migth be applied in-between

    YAML Example:

      all: 
        name1: "default value1"
        name2: "default value2"
        name3: "default value3"

      webservers: 
        name2: "overriden by all hosts belonging to webservers"

      host003: 
        name1: "overriden by host003 only"
    """
    # initialize data map
    data = {}
    if isinstance (allvars, dict):
      data = allvars
    elif isinstance (allvars, basestring):
      with open(allvars, 'rb') as f:
        data = yaml.load (f)
    else:
      raise Exception ("Expecting parameter to be a dict or a path to a JSON/YAML file")

    # set data
    hosts = set(self.hosts())

    # first 'all', then every other pattern, then the hosts
    group_patterns = [p for p in data.keys() if (p != 'all') and (p not in hosts)]
    sorted_patterns = ['all'] + group_patterns + list(hosts)

    for pattern in sorted_patterns:
      for host in self.inventory.get_hosts('%s:&%s' % (self.pattern, pattern)):
        for k,v in data.get(pattern, {}).iteritems():
          self.vars[host.name][k] = v

    return

  def setvar (self, var_name, var_value):
    """ Sets a variable value on all the hosts
    """
    for host in self.hosts():
      self.vars[host][var_name] = var_value
    return

  def sethostvars (self, var_name, host_vars):
    """ Sets a variable value for all the hosts defined in host_vars
    """
    for host in self.hosts():
      if host in host_vars:
        self.vars[host][var_name] = host_vars[host]
    return

  def hostvar (self, host, name, default = None):
    """ Gets the value associated to a variable on a specific host
    """
    return self.vars[host].get (name, default)

  def hostvars (self, host):
    """ Return all variables for given host
    """
    return self.vars[host]

  def set_fact (self, **kwargs):
    """ Set a new fact for the current group of servers
    """
    return self.module_functor ('set_fact')(**kwargs)

  def groups (self):
    """ Returns all group names from the inventory
    """
    return [g.name for g in self.inventory.get_groups()]

  def hosts (self, pattern = 'all'):
    """ Returns a list containing all the host names, or the 
    host names associated to given pattern
    """
    return [h.name for h in self.inventory.get_hosts('%s:&%s' % (self.pattern, pattern))]

  def module_functor (self, module_name):
    """ Returns a function that will call ansible
    """
    def ansible_runner (*args, **kwargs):
      module_args  = None
      complex_args = copy.copy (kwargs)

      if (len(args) == 1):
        if (module_name in FREE_STYLE_MODULES):
          module_args = args[0]
        else:
          complex_args['name'] = args[0]

      elif (len(args) > 1):
        raise Exception ("Only one positional argument supported (name)")

      # remove extra runner kwargs from the complex_args itself
      if 'runner_kwargs' in complex_args:
        del complex_args['runner_kwargs']

      # default runner kwargs, that can be overriden by 'runner_kwargs'
      # to provide total control
      runner_kwargs = {
        "inventory"    : self.inventory,
        "module_name"  : module_name,
        "complex_args" : complex_args,
        "setup_cache"  : self.facts,
        "vars_cache"   : self.vars, # vars are expanded before execution
        "pattern"      : self.pattern,
        "no_log"       : True,
        "forks"        : 10
      }

      if module_args:
        runner_kwargs["module_args"] = module_args

      if self.private_key_file:
        runner_kwargs["private_key_file"] = self.private_key_file

      runner_kwargs.update (kwargs.get('runner_kwargs', {}))

      runner = ansible.runner.Runner(**runner_kwargs)
      response = AnsimpleRensponse(runner.run ()) 

      # save response facts
      for host, facts in response.facts.iteritems():
        self.facts[host].update (facts)
      self.last = response

      return (response)

    return ansible_runner

  def j2template (self, template_file):
    """ Expand given template for each host
    """
    expansion = {}
    for host in self.hosts():
      expansion[host] = ansible.utils.template.template_from_file (
        None,
        template_file,
        self.hostvars(host)
      )
    return expansion


class AnsimpleRensponse:
  """ Simplify accessing server data

  Example:

    >>> res = ans.ping()
    >>> for host in res.hosts():
    >>>    print res[host].changed
    >>> res.all_changed
    True

  Internal variables:
  
    not_answered = {
      'host' : {'msg' : '...', 'failed' : True}
    }

    answered = {
      'host' : {
        'changed'       : True|False,
        'invocation'    : { 'module_name' : ..., 'args' : ... },
        'ansible_facts' : {...},
        ...
      }
    }
  """
  def __init__ (self, response):
    self.not_answered = response.get ('dark', {})
    self.answered     = response.get ('contacted', {})
    self.facts        = {}

    # fill facts per host
    for host, host_response in self.answered.iteritems():
      self.facts[host] = host_response.get ('ansible_facts', {})

    # initialize several variables useful when verifying things
    self.total_count   = len (self.answered) + len(self.not_answered)
    self.err_count     = (
      sum(1 for res in self.answered.values() if res.get ('failed', False)) +
      sum(1 for res in self.not_answered.values() if res.get ('failed', False))
    )
    self.ok_count      = self.total_count - self.err_count
    self.changed_count = (
      sum(1 for res in self.answered.values() if res.get ('changed', False)) +
      sum(1 for res in self.not_answered.values() if res.get ('changed', False))
    )

    self.all_ok      = (self.err_count == 0)
    self.some_ok     = (self.all_ok) or (self.ok_count > 0)
    self.none_ok     = ((self.ok_count == 0) and (self.err_count > 0))
    
    self.all_failed  = self.none_ok
    self.some_failed = (self.err_count > 0)
    self.none_failed = self.all_ok

    self.all_changed  = (self.total_count == self.changed_count)
    self.some_changed = (self.changed_count > 0)
    self.none_changed = (self.changed_count == 0)
    return

  def empty (self):
    """ Return TRUE if there was no server
    """
    return (self.total_count == 0)

  def __getitem__ (self, host):
    """ Returns the response from the hosts either if they answered or not
    """
    if host in self.answered:
      return self.answered [host]
    return self.not_answered.get (host, {})

  def hosts(self):
    """ Return the host names of all the servers that we tried to contact,
    wether the call succeeded or failed
    """
    return list(set(self.not_answered.keys() + self.answered.keys()))

  def getSuccessCount (self):
    """ Returns the number of servers that succeeded.
    Please note that this value can be 0 and still there can be no failures.
    """
    return self.ok_count

  def success (self):
    """ Returns True when all servers succeed
    """
    return self.all_ok

  def failed (self):
    """ Returns True when any server failed
    """
    return self.all_failed

  def __repr__ (self):
    s = []
    for host in self.hosts():
      s.append ('\n---- %s: ' % host)
      s.append (
        json.dumps(
          self[host], sort_keys=True, indent=2, separators=(',', ': ')
        )
      )
    return '\n'.join (s)
