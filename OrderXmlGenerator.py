# -*- coding: utf-8 -*-
import datetime
import uuid as u
import paramiko
from lxml import etree, objectify
from random import randint
import jpype
import jaydebeapi
from scp import SCPClient

c_server = "192.168.111.32"
c_port = 22
c_user = "abrovin"
c_password = "f4f247nwc!"
c_amount = 777
c_manufacture = 101
c_sector = "01"
c_exchange_folder = "/var/lib/aks-adapter-sap/exchange/soap/in"


def exec_script():
    j_home = jpype.getDefaultJVMPath()
    jpype.startJVM(j_home, '-Djava.class.path=ojdbc8.jar')
    conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver',
                              'jdbc:oracle:thin:system/oracle@autotesting.bs.local:1521:xe')
    curs = conn.cursor()

    # todo: сделать конфигурирование департамента
    curs.execute(
        '''select 
            fsd.machine_model_id, fsd.machine_model_ext_id,
            d.ext_id as department_extId,
            oc.ext_id as oc_Ext_id,
            pu.name as component_full_name,
            pu.plain_name as component_name,
            pu.code as component_code,
            oc.stage as component_stage, 
            fsd.operation_number as operation_extId,
            op.name as operation_name,
            fs.route_code as routeCode
        from 
            AKS_ORDER_PROD.flow_sheet_detail fsd 
            inner join AKS_ORDER_PROD.flow_sheet fs on fsd.flow_sheet_id = fs.id 
            inner join AKS_ORDER_PROD.order_component oc on fs.order_component_id = oc.id
            inner join AKS_ORDER_PROD.order_operation op on fsd.order_operation_id = op.id
            inner join AKS_ORDER_PROD.product_unit pu on pu.id = oc.product_unit_id
            inner join AKS_ADAPTER_SAP.department d on d.id = 1
        where 
            fs.department_id = 1 and
            fs.has_tool_operations = 1 and
            fsd.machine_model_id is not null''')
    res = curs.fetchall()
    curs.close()
    conn.close()
    print("Data received from DB.")
    return res


def create_ssh_client(server, port, user, password):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, port, user, password)
    return client


# region create xml nodes


def create_e1afvol(data):
    appt = objectify.Element("E1AFVOL")
    appt.VORNR = data["VORNR"]
    appt.LTXA1 = data["LTXA1"]
    appt.ARBID = data["ARBID"]
    appt.MGVRG = data["MGVRG"]
    appt.MEINH = data["MEINH"]
    appt.LMNGA = data["LMNGA"]
    return appt


def create_e1jstkl(data):
    appt = objectify.Element("E1JSTKL")
    appt.STAT = data["STAT"]
    return appt


def create_e1afpol(data):
    appt = objectify.Element("E1AFPOL")
    appt.VERID = data["VERID"]
    return appt


def create_appt(data):
    appt = objectify.Element("Item")
    appt.AUFNR = data["AUFNR"]
    appt.AUART = data["AUART"]
    appt.GSTRS = data["GSTRS"]
    appt.GLTRS = data["GLTRS"]
    appt.LGORT = data["LGORT"]
    appt.PLNBEZ = data["PLNBEZ"]
    appt.MATXT = data["MATXT"]
    appt.HF_DSE_NAME = data["HF_DSE_NAME"]
    appt.HF_DSE_KTD = data["HF_DSE_KTD"]
    # appt.HF_DSE_NAME_H = data["HF_DSE_NAME_H"]
    appt.BMENGE = data["BMENGE"]
    appt.BMEINS = data["BMEINS"]
    appt.IGMNG = data["IGMNG"]
    appt.APRIO = data["APRIO"]
    return appt


# endregion


def create_xml(p_component_id, p_full_name, p_plain_name, p_code, p_stage, p_verid, p_operation_code, p_operation_name,
               p_tool_ext_id):
    # Создаем XML файл.
    xml = '''<SOAP:Envelope xmlns:SOAP='http://schemas.xmlsoap.org/soap/envelope/'>
        <SOAP:Header/>
        <SOAP:Body>
        <ProductionOrderRequest 
        xmlns:prx='urn:sap.com:proxy:EPR:/1SAI/TAS6CA1BE8F0794C74DF05D:731'>
        <HeaderSection>
        <UUID>{0}</UUID>
        <Sender>KSAUPKK_ERP</Sender>
        <Recipients>SUIKP</Recipients>
        <CreationDateTime>{1}</CreationDateTime>
        </HeaderSection>
        <DataSection>
        </DataSection>
        </ProductionOrderRequest>
        </SOAP:Body>
        </SOAP:Envelope>
    '''.format(u.uuid1(), "%sZ" % datetime.datetime.now().isoformat(timespec="seconds"))

    root = objectify.fromstring(xml)
    nav = root.find(".//DataSection")

    # заполнение данных
    order_name = "T{0}-{1}".format(datetime.datetime.now().strftime("%y%m%d"), randint(1, 999))
    sector_hex = "{0:x}".format(int(c_sector))

    item = create_appt({
        "AUFNR": order_name,
        "AUART": "P%s" % c_manufacture,
        "GSTRS": datetime.datetime.now().strftime("%Y-%m-%d"),
        "GLTRS": datetime.datetime.now().strftime("%Y-%m-%d"),
        "LGORT": "{0}{1}".format(c_manufacture, sector_hex),

        "PLNBEZ": p_component_id,
        "MATXT": p_full_name,
        "HF_DSE_NAME": p_plain_name,
        "HF_DSE_KTD": p_code,
        "HF_DSE_NAME_H": p_stage,
        "BMENGE": c_amount,
        "BMEINS": "ST",
        "IGMNG": "0.0",
        "APRIO": "",
    })
    nav.append(item)

    # вариант изготовления ДСЕ
    eiafpol = create_e1afpol({"VERID": p_verid})
    nav = root.find(".//APRIO")
    nav.addnext(eiafpol)

    # статус заказа
    e1jstkl = create_e1jstkl({"STAT": "I0002"})
    nav = root.find(".//E1AFPOL")
    nav.addnext(e1jstkl)

    # операция по заказу
    e1afvol = create_e1afvol({
        "VORNR": p_operation_code,
        "LTXA1": p_operation_name,
        "ARBID": p_tool_ext_id,
        "MGVRG": c_amount,
        "MEINH": "ST",
        "LMNGA": "0.0",

    })
    nav = root.find(".//E1JSTKL")
    nav.addnext(e1afvol)

    # удаляем все lxml аннотации.
    objectify.deannotate(root)

    # удаляем лишние неимспеисы.
    nav = root.find(".//Item")
    etree.cleanup_namespaces(nav)
    obj_xml = etree.tostring(root,
                             encoding="UTF-8",
                             pretty_print=True,
                             xml_declaration=True)

    order_file_name = "Order_%s.xml" % datetime.datetime.now().isoformat()
    try:
        with open(order_file_name, "wb") as xml_writer:
            xml_writer.write(obj_xml)
    except IOError:
        pass
    print("XML file {0} created".format(order_file_name))

    return order_file_name


# копирование файла в директорию aks-sap-adapter для обмена
def copy_to_server(order_file_name):
    ssh = create_ssh_client(c_server, c_port, c_user, c_password)
    scp = SCPClient(ssh.get_transport())
    scp.put(order_file_name, c_exchange_folder, False)
    print("{0} was copied".format(order_file_name))


def main():
    # Получить данные из БД.
    items = exec_script()

    # Сгенерировать xml для полученных данных.
    j = 0
    while j < 15 and len(items) > j:
        i = items[j]
        j = j + 1
        tool_ext_id = i[1]
        component_id = i[3]
        full_name = i[4]
        plain_name = i[5]
        code = i[6]
        if i[7] == "None":
            stage = ""
        else:
            stage = i[7]
        operation_code = i[8]
        operation_name = i[9]
        verid = i[10]
        print(i)
        file_name = create_xml(component_id, full_name, plain_name, code, stage, verid, operation_code, operation_name,
                               tool_ext_id)

        # скопировать xml на сервер.
        copy_to_server(file_name)
# todo: добавить возможность выбора цеха и сектора.


if __name__ == '__main__':
    main()
