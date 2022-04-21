import datetime
import sys
import os
import getpass

def box_mid(string,box_length,line_ending = '\r\n'):
    return '* ' + string + ' ' * (box_length - 3 - len(string)) + '*' + line_ending

class Logger(object):
    DEV = 'DEV'
    WARNING = 'WARNING'
    CRITICAL_ERROR = 'C_ERR'
    ERROR = 'ERR'
    def __init__(self, module, path, config, **kwargs):
        if 'user' in kwargs:
            self.user = kwargs['user']
        else:
            self.user = getpass.getuser()
        self.path = os.path.join(os.path.join(path,'%s.%s'%(module,self.user)))
        self.module = module.upper()
        self.additional_params = kwargs
        self.config = config
        try:
            if not os.path.exists(self.path):
                self.file_handle = open(self.path, 'w+')
                self.file_handle.write('*'*100+'\r\n')        
                self.file_handle.write(box_mid('%s LOGS'%(self.module),100))
                self.file_handle.write(box_mid('USER: %s'%(self.user),100))
                if len(kwargs) > 0:
                    self.file_handle.write(box_mid('CONFIG INFO',100))        
                    for key in self.additional_params.keys():
                        self.file_handle.write(box_mid(' ' * 4 + '-- %s: %s'%(key,str(self.additional_params[key])),100))
                self.file_handle.write('*'*100+'\r\n')      
                
            else:
                self.file_handle = open(self.path, 'a')
            self.file_handle.write("#################### RUN AT: " + str(datetime.datetime.now()) + " ####################\n" )  
        except:
            print('Invalid file path \'%s\', home directory will be used to store log file'%(path))
            self.path = '/home/%s/%s.%s'%(getpass.getuser(),module,self.user)
            self.file_handle = open(self.path,'w+')
            self.file_handle.write('*'*100+'\r\n')        
            self.file_handle.write(box_mid('%s LOGS'%(self.module),100))
            self.file_handle.write(box_mid('USER: %s'%(self.user),100))
            if len(kwargs) > 0:
                self.file_handle.write(box_mid('CONFIG INFO',100))        
                for key in self.additional_params.keys():
                    self.file_handle.write(box_mid(' ' * 4 + '-- %s: %s'%(key,str(self.additional_params[key])),100))
            self.file_handle.write('*'*100+'\r\n')
            self.file_handle.write("#################### RUN AT: " + str(datetime.datetime.now()) + " ####################\n" )        

    def log(self,issue_type,message,**kwargs):
        now = str(datetime.datetime.now())
        config_info = '| '
        if len(kwargs) > 0:
            for key in kwargs.keys():
                config_info += '[%s: %s] '%(key,str(kwargs[key]))
        else:
            config_info = ''
        self.file_handle.write('[%s] - [%s] %s %s\r\n'%(now,issue_type.upper(),message,config_info))

    def log_wrapper(self, exception):
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_string = """Error on file: {}, [line]: {}, [type]: {}, [description]: {}""".format(exc_tb.tb_frame.f_code.co_filename, exc_tb.tb_lineno, type(exception).__name__, exception)
        self.log(self.ERROR, error_string)       

    def close_log(self):
        self.file_handle.close()

def logger_dev_test():
    logger = Logger('test','/home/admin/PDA-master/prog_env_dev',kwargtest='a')
    logger.log('error','message',file='std_trans.py',something='nothing')
    logger.log('warning','message')
    logger.log('warning','message')
    logger.log('issue','conformance issue')
    logger.close_log()


if __name__=='__main__' and __package__ is None:
    __package__ = 'logger'