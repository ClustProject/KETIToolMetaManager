import sys
import os
import json
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

def DBNameToDomainSubDomainName(dbname):
    domain = dbname.split("_", maxsplit=1)[0]
    sub_domain = dbname.split("_", maxsplit=1)[1]
    return domain, sub_domain


class metaGenerator():
    def __init__(self, metaUploadParam):
        self.path = metaUploadParam["filePath"]
        self.filename = metaUploadParam["fileName"]
        self.ms_list = metaUploadParam["measurementsName"]
        self.domain = metaUploadParam["dbName"]
        self.sub_domain = metaUploadParam["collectionName"]

    def check_default_info_inMeta(self, meta):
        # 각 데이터가 table_name, domain, subDomain 정보가 필수적으로 필요한가? 아래 코드는 오류가 있을 수 있음
        if 'table_name' not in meta.keys():
            meta['table_name'] = self.ms_list # 이부분이 불명확함,  msList 여러개에 대해서 작동할 경우에 대해 체크 필요
        if 'domain' not in meta.keys():
            meta["domain"] = self.domain
        if 'subDomain' not in meta.keys():
            meta["subDomain"] = self.sub_domain
        return meta

    # JISU
    # Algorithm에 대해서 한번 더 컨펌
    # 아래 함수를 쓸 경우 (조금 정리하면) FilePath가 없다면, 사용자의 수기 input만 받아서 처리하는 것도 이 펑션 하나로 모두 합칠 수 있음
    # generatorMode는 삭제 하고 아래 function으로 이름 교체함

    def get_metadata_by_condition(self, additional_meta = None):
        """
        * 처리 플로우

        1. filePath 정보 유무 확인
            - filePath가 있을 경우 읽고 fileMeta 생성
            - filePath가 없을 경우 fileMeta {}로 초기화

        2. ms 명확히 명시되었는지 확인
            - ms가 명확히 존재할 경우
                - additional meta 정보 확인
                    - 있을 경우 특정 MS에 대해 fileMeta + additonal meta 정보를 합쳐 생성
                    - 없을 경우 빈 정보
                - 필수 meta key를 검사하고 update
            - ms가 명확히 존재하지 않을 경우 <- 이부분은 별도 function으로 하는게 더 나을 수 있음
                - 여러 MS에 대해 update하는 경우로 다수의 MS에 대해 + adiitioinal meta 정보를 합쳐 생성
                - 필수 meta key에 대한 업데이트 (필요한가?)

        :param additional_meta: 덧붙이고 싶은 Meta 정보
        :type basic_meta: dictionary
        
        :returns: 최종 메타 정보
        :rtype: dictionary or arrary  <------ 맞는가? return이 여러 타입이면 좋지 않음, 현재 버젼은 요렇게 보임
        --> 그러므로 function을 하나 ms에 대해 할 것인지, 여러 셋에 대해 할것인지로 두개로 쪼개는게 좋음 (현재는 정확히 파악이 안되어 하나로 뭉쳤음)
        """
        # 이 부분에 filename이 있는지 없는지를 체크함
        if self.filename:
            fileMeta = self.read_json_file_meta(self.path, self.filename)

        else:
            fileMeta = {}

        if self.ms_list: #only one ms
            if additional_meta:
                fileMeta.update(additional_meta)
            meta = self.check_default_info_inMeta(fileMeta)
        else: # all ms based on file_meta information
            #filemeta가 array인지를 확인하는 구문이 있어야함 fileMeta가 잘못 입력되는 경우도 있을테니 TODO
            # 아래 코드는 무조건 self.ms_list가 None일 경우 들어오는 filemeta는 array 인 것을 가정하고 작성되어있음
            # 이런 코드는 좋지 않음
            # else 부분부터 별도의 function으로 쪼개는 것도 좋음
            meta = []
            for oneFileMeta in fileMeta:
                if additional_meta:
                    oneFileMeta.update(additional_meta)
                oneFileMeta = self.check_default_info_inMeta(oneFileMeta) # 이 부분 필요한가 혹은 이부분이 중요하다면 체크하는 함수를 다시 만들거나 보강해야함
                meta.append(oneFileMeta)

        return meta

    def read_json_file_meta(self, filePath, fileName):
        with open(os.path.join(filePath, fileName), "r", encoding="utf-8") as meta:
            file_meta = json.load(meta)
        return file_meta
    
