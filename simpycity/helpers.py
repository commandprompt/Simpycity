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
                container = container.obj
            
            
            for arg in self.sim.args:
                if arg in container.col:
                    argmap[arg] = container.col[arg]
                elif setarg:
                    raise AttributeError("More than 1 unmappable argument in property.")
                else:
                    setarg = True
                    argmap[arg] = args[0]
            
            try:
                handle = obj.handle
                argmap['options'] = {}
                argmap['options']['handle'] = handle
            except:
                pass
                
            if 'options' in argmap:
                argmap['options']['fold_output'] = True
            else:
                argmap['options'] = {}
                argmap['options']['fold_output'] = True
                
            # Let's add some limited support for output folding here.
            rs = self.sim(**argmap)
        else:
            # no mappings required.
            return self.sim()
        

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
        
    return property(_PropertyContainer)
    
def classproperty(definer):

    class _PropContainer(definer):
        pass

    return _PropContainer