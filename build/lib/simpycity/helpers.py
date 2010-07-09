from simpycity.core import d_out

class sprop(object):
    """A simpycity Property object, designed to handle argument mapping
    of Simpycity objects when those objects are pretending to be object
    properties. This allows for trickery such as property() to work correctly
    with Simpycity structures.
    
    This will eventually be integrated into meta_query/SimpleModel.
    """
    def __init__(self, obj):
        self.sim = obj
    
    def __call__(self, container, *args):
        # And now, we map container values to the simpycity function 
        # arguments, and all is well.
        if len(self.sim.args) >= 1:
            # do the mapping.
            # Given we're assuming that any request here is going to be a
            # single value - basically a foo = val state - we can assume
            # that we're only ever going to get one value in *args.
            # Ergo, 
            
            if len(args) > 1:
                raise AttributeError("Can only have 1 argument, got %s" % args)
            
            # Now, on to the work.
            argmap = {}
            setarg = False
            
            if '_PropertyContainer' in [x.__name__ for x in type(container).mro()]:
                d_out("sprop.__call__(): Found a _PropertyContainer, setting container..")
                container = container.obj
            
            
            for arg in self.sim.args:
                if arg in container.col:
                    d_out("sprop.__call__(): set %s to %s.." % (arg, container.col[arg]))
                    argmap[arg] = container.col[arg]
                elif setarg:
                    d_out("sprop.__call__(): Failed to set %s..." % (arg))
                    raise AttributeError("More than 1 unmappable argument in property in %s" % container.__class__)
                else:
                    setarg = True
                    try:
                        argmap[arg] = args[0]
                    except IndexError, e:
                        raise TypeError("Insufficient mappable args in %s" % container.__class__)
                    d_out("sprop.__call__(): set %s to %s.." % (arg, args[0]))
            
            try:
                d_out("sprop.__call__(): Attemting to get handle..")
                handle = container.handle
                d_out("sprop.__call__(): Got handle. Setting argmap[options]")
                argmap['options'] = {}
                argmap['options']['handle'] = handle
            except Exception, e:
                d_out("sprop.__call__(): Couldn't get handle, got exception %s" % e)
                pass
                
            if 'options' in argmap:
                argmap['options']['fold_output'] = True
            else:
                d_out("sprop.__call__(): Didn't find options; creating.")
                argmap['options'] = {}
                argmap['options']['fold_output'] = True
                
            # Let's add some limited support for output folding here.
            rs = self.sim(**argmap)
            return rs
        else:
            # no mappings required.
            return self.sim(options=dict(fold_output=True))
        

def multiproperty(definer):
    
    class _PropertyContainer(object):
        attrs = {}
        def __init__(self,obj):
            self.obj = obj
            self.values = definer()
                                
            for name in self.values:
                setattr(_PropertyContainer, name, property(**self.values[name]()))
        def __repr__(self):
            return "Subproperty manager instance"
            
    c = _PropertyContainer
        
    return property(c)
    
# def classproperty(definer):
# 
#     class _PropertyContainer(definer):
#         pass
# 
#     return _PropContainer
    
def classproperty(definer):
    a = definer()
    def cont(parent):
        # a.col = {} 
        # a.col['user_id'] = obj.user_id
        return a
    return property(cont)