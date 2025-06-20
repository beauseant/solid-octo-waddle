import glob
import fitz
import pathlib
import multiprocessing


#Propias:
from .pdf_parser import PDFParser
#from .summarizer import Summarizer
from .multi_column import column_boxes

import pandas as pd

class processPDF:

    __inputDir__    = ''
    __outputDir__   = ''
    __workers__     = 0
    __results__     = []
    __listFiles__   = []
    __parquetFile__ = []

    def __init__(self, parquetFile, outputDir, workers): 
        self.__parquetFile__   = parquetFile
        self.__outputDir__  = outputDir
        self.__workers__    = workers


    def __getAllDocs ( self ):
        #return glob.glob( directory + '/**/*.pdf', recursive=True)
        data=pd.read_parquet(self.__parquetFile__)
        return (
                list (data.query('texto_administrativo.str.startswith ("[CORRECTO")')['path_administrativo']) +
                list (data.query('texto_tecnico.str.startswith ("[CORRECTO")')['path_tecnico'])
                )

    def getListFiles (self):
        return self.__listFiles__
    
    #for test:
    @staticmethod
    def processPDFDummy ( path ):
        time.sleep (1)
        return ('el nombre del archivo es %s' % path)

    #simple extract text:
    @staticmethod
    def processPDFSimple ( path ):
        doc = fitz.open( path )
        data = ''
        for page in doc:
            bboxes = column_boxes(page, footer_margin=50, no_image_text=True)
            for rect in bboxes:
                data += (page.get_text(clip=rect, sort=True))

            
        return data 
    
    @staticmethod
    def processPDF (path, savedir, summary=False ):

        try:
            # Define the path to the PDF file
            pdf_file = pathlib.Path (path)
            
            # Create a directory to save the extracted content (one directory per PDF file)
            path_save = pathlib.Path(savedir) / pdf_file.stem
            path_save.mkdir(parents=True, exist_ok=True)
            path_save.joinpath("images").mkdir(parents=True, exist_ok=True)
            path_save.joinpath("tables").mkdir(parents=True, exist_ok=True)
            

            
            # Create a PDFParser and parse the PDF file
            pdf_parser = PDFParser(
                generate_img_desc=False,
                generate_table_desc=False,
            )
            pdf_parser.parse(pdf_path=pdf_file, path_save=path_save)
            
            if summary:
                # Create a Summarizer with the default parameters and summarize the PDF file
                summarizer = Summarizer(
                    model_type="openai",
                    model_name="gpt-4",
                )                            
                summarizer.summarize(pdf_file=pdf_file, path_save=path_save)
                                    
            #file ok
            return ({'path':path,'result':True})
        except Exception as E:
            #file ko            
            #return ({'path':path,'result':False})
            return ({'path':path,'result':str(E)})
              

    def processFiles ( self ):
        
        listFiles = self.__getAllDocs ()
        self.__listFiles__ = listFiles

        saveDirList = [self.__outputDir__] * len(listFiles)

        

        num_processes = multiprocessing.cpu_count() if self.__workers__ == 0 else self.__workers__
        print ('using %s workers to process %s files' % (num_processes, len(listFiles)))

        with multiprocessing.Pool(processes=num_processes) as pool:
            results = pool.starmap(processPDF.processPDF, zip(listFiles, saveDirList))
            if any(results):
                self.__results__ = results
                return results

        return False    
