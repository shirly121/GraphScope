#include <google/protobuf/util/json_util.h>
#include "physical.pb.h"

using namespace google::protobuf;
using namespace google::protobuf::util;

void convert(const physical::Scan &scan)
{
    std::cout << "scan: " << scan.DebugString() << std::endl;
}

void convert(const physical::EdgeExpand &expand)
{
    std::cout << "expand: " << expand.DebugString() << std::endl;
}

void convert(const physical::GetV &getV)
{
    std::cout << "getV: " << getV.DebugString() << std::endl;
}

void convert(const physical::PathExpand &pxd)
{
    std::cout << "pxd: " << pxd.DebugString() << std::endl;
}

void convert(const physical::Project &project)
{
    std::cout << "project: " << project.DebugString() << std::endl;
}

void convert(const algebra::Select &select)
{
    std::cout << "filter: " << select.DebugString() << std::endl;
}

void convert(const algebra::GroupBy &group)
{
    std::cout << "group: " << group.DebugString() << std::endl;
}

void convert(const physical::Join &join)
{
    std::cout << "join: " << join.DebugString() << std::endl;
    convert(join.left_plan());
    convert(join.right_plan());
}

void convert(const algebra::OrderBy &order)
{
    std::cout << "order: " << order.DebugString() << std::endl;
}

void convert(const physical::PhysicalPlan &plan)
{
    for (int i = 0; i < plan.plan_size(); i++)
    {
        auto node = plan.plan(i);
        switch (node.opr().op_kind_case())
        {
        case physical::PhysicalOpr_Operator::OpKindCase::kScan:
            convert(node.scan());
            break;
        case physical::PhysicalOpr_Operator::OpKindCase::kEdge:
            convert(node.edge_expand());
            break;
        case physical::PhysicalOpr_Operator::OpKindCase::kVertex:
            convert(node.get_v());
            break;
        case physical::PhysicalOpr_Operator::OpKindCase::kPath:
            convert(node.path_expand());
            break;
        case physical::PhysicalOpr_Operator::OpKindCase::kProject:
            convert(node.project());
            break;
        case physical::PhysicalOpr_Operator::OpKindCase::kJoin:
            convert(node.join());
            break;
        default:
            std::cout << "unknown node type: " << node.type_case()
                      << std::endl;
        }
    }
}

std::string readFile(const string &filename)
{
    std::ifstream file(filename);
    if (!file.is_open())
    {
        cerr << "Error opening file: " << filename << endl;
        return "";
    }
    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
}

int main(int argc, char **argv)
{
    std::string plan_json_file = argv[1];
    // Read JSON file
    string json_content = readFile(plan_json_file);
    if (json_content.empty())
    {
        return 1;
    }
    // Initialize protobuf message

    physical::PhysicalPlan config;

    // Convert JSON to Protobuf
    JsonParseOptions options;
    Status status = JsonStringToMessage(json_content, &config, options);
    if (!status.ok())
    {
        cerr << "Failed to parse JSON: " << status.message() << endl;
        return 1;
    }

    convert(config);

    return 0;
}