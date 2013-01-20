The Focuser class is a python interface to rainbow from Andrew McCallum's [Bow Toolkit](http://www.cs.cmu.edu/~mccallum/bow/). You must have already trained rainbow. At the moment, the path to the model's parent directory is hard-coded into the program. For example, if you have a model in /home/username/models/free_trade you would replace /PATH/TO/MODELS/ with /home/username/models/. You could then create and use a new Focuser like this

    f = Focuser('free_trade')
    cutoff = 0.9
    score = f.classify("I love free trade!")
    if score >= cutoff: print "Document appears relevant"

If you do not specify a hostname or specify localhost, Focuser will try to launch rainbow itself. If you specify a different hostname, Focuser will try to connect to an already running rainbow.

If you do not specify a port, Focuser assumes that its first instance should use 2000, its second instance should use 2001, and so on. The starting point of 2000 is arbitrary and can be changed, the important thing is that no program is already using that port.

If an error occurs, a FocuserError exception will be raised that has more details. If rainbow was started locally by Focuser, there will also be a log file with more information. The log file name is the port number and has the extension log. For instance, a rainbow running on port 2000 will log to 2000.log

If Focuser starts rainbow, it will also try to stop it. This occurs when the Focuser instance is garbage collected. However, this is not guaranteed to happen when the python interpreter exits, so you may have to manually identify and kill leftover rainbows.
