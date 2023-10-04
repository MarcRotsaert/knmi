""" Verzameling classes voor omgang met vector data"""



import numpy as np
def degn_cart2naut_cf(u,v):
    degc= np.arctan2(v,u)*180/np.pi
    degn=np.mod(270-degc,360)
    return degn

def degn_cart2naut_gt(u,v):
    """ cartesisch naar nautisch, Going to  """
    degc= np.arctan2(v,u)*180/np.pi
    degn=np.mod(90-degc,360);
    return degn

def naut2pol_gt(degn):
    #print('bla')
    #print(degn)
    phi=np.mod(90-degn,360)
    return phi
    
def naut2pol_cf(degn):
    phi=np.mod(270-degn,360)
    return phi

def pol2uv(phi,vel):
    u=vel*np.cos(2*np.pi*phi/360)
    v=vel*np.sin(2*np.pi*phi/360)
    #print u,v
    return u,v

def vel(u,v):
    vel=(u**2+v**2)**0.5
    return vel


class _U1d:
    """1-dimensionale vector"""
    def __init__(self,val,ric):
        #print ric
        self.Val = np.ma.masked_array(val)
        if ric.upper()=='GT' or ric.upper()=='CF':
            self.Ric= ric#'CF'=Coming from(Waves, Wind); 'GT'='Going To (Currents)'
        else:
            raise TypeError('Opties voor variable ric: CF of GT')

class Cart(_U1d):
    """
    Cartesisch 
    """
    #def __init__(self,u,v,**kwargs):
    def __init__(self,u,v,ric='GT'):
        assert(u.__class__==v.__class__),"u en v snelheid uit verschillende bron"
        uori=u
        if isinstance(u,_U1d)==False:
            u= np.ma.masked_array(u).flatten(); 
            v= np.ma.masked_array(v).flatten(); 
            #print u
            #uobj=_U1d(u,kwargs['ric'])
            #vobj=_U1d(v,kwargs['ric'])
            uobj=_U1d(u,ric)
            vobj=_U1d(v,ric)
            self.U=uobj;self.V=vobj
        else:
            raise Exception
        if uobj.Ric=='CF':
            self.degn_cart2naut=degn_cart2naut_cf(self.U.Val,self.V.Val)
        else:#uobj.Ric=='GT':
            self.degn_cart2naut=degn_cart2naut_gt(self.U.Val,self.V.Val)
        #self.vel=vel(self.U.Val,self.V.Val)
        #print uori
        vflat=vel(self.U.Val,self.V.Val)
        try:
            self.vel=vflat.reshape(uori.shape)
        except AttributeError:
            self.vel=vflat.reshape(np.array([uori]).shape)

    def cart2naut(self):
        vel=self.vel
        deg=self.degn_cart2naut
        return vel,deg

    def quiver(self,X,Y,*args,**kwargs):
        from pylab import quiver
        assert self.U.Val.shape==X.shape==Y.shape
        if self.Ric=='CF':
            kwargs.update({'pivot':'tip'})
        else:
            kwargs.update({'pivot':'tail'})
        u= self.U.Val
        v=self.V.Val
        quiver(X,Y,u,v,*args,**kwargs)

class Veldir:
    def __init__(self,vel,degn,ric):
        """
        Vectoren in vorm snelheid/richting
        Input:
            vel: snelheid (array of numerieke waarde)
            degn: richting (array of numerieke waarde)
            rich: definitie richting ([CF/GT] GT= Going To, CF=Coming from
        """
        assert(vel.__class__==degn.__class__),"snelheid en graden uit verschillende bron. Dit is niet toegestaan"
        assert ['GT','CF'].count(ric)==True,'richting definitie geef op \'GT of \'CF'

        if isinstance(vel,np.ndarray):
            self.Vel= vel
            self.Degn= degn

        else:
            self.Vel=np.array(vel)
            self.Degn= np.array(degn)
        assert self.Vel.shape==self.Degn.shape    
        self.Ric=ric

        if self.Ric=='CF':
            self.naut2pol=naut2pol_cf(self.Degn)
        else:
            self.naut2pol=naut2pol_gt(self.Degn)
        self.pol2uv=pol2uv

    def veldir2uv(self):
        phi=self.naut2pol
        u,v=self.pol2uv(phi,self.Vel)
        return u,v

    def quiver(self,X,Y,*args,**kwargs):
        from pylab import quiver
        assert self.Vel.shape==X.shape==Y.shape
        if self.Ric=='CF':
            kwargs.update({'pivot':'tip'})
        else:
            kwargs.update({'pivot':'tail'})
        u,v=self.veldir2uv()
        quiver(X,Y,u,v,*args,**kwargs)

